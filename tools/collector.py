#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创作者情报工作站 V1.0 —— 实时采集与入库管线 (collector)

合规边界（务必遵守，勿改）：
  * 仅请求「免登录、公开可访问」的数据接口；
  * 不携带任何登录态 / cookie，不复用任何账号会话；
  * 不生成或伪造 a_bogus / X-Bogus / WBI 等反爬签名；
  * 不触发、不绕过验证码 / 二次验证 / 设备校验；
  * 不采集需授权、含实名 / 手机号等个人信息的数据；
  * 任一数据源失败做隔离，绝不影响其他源与本轮整体。

数据源（14 源 / 7 平台，全部实测返回真实公开数据；详见 sources_ext.SOURCE_META）：
  快线 120s：抖音热点榜、抖音镜像校验、微博热搜、百度实时热搜、
             今日头条热榜、网易热点新闻、知乎热搜词、B站热搜词
  慢线 300s：B站全站排行榜、B站热门、B站分区排行榜
  慢线 600s：B站关键词搜索（WAF 较严，主动降频）
  日更 3600s：百度垂直分榜、B站入站必刷（变化慢，没必要每轮抓）
每个源有独立 min_interval_seconds；cron 每 2 分钟触发一轮，未到间隔的源
写一条 status='scheduled_skip' 的审计记录后跳过 —— 分级调度既提升热榜实时性，
又不让风控敏感的 B站作品接口被高频打。

用法：
  python3 tools/collector.py --once                      # 跑一轮
  python3 tools/collector.py --rounds 3 --interval 60   # 连跑 3 轮，验证时序
  python3 tools/dashboard.py                            # 起可视化看板
  python3 tools/dashboard.py --export                   # 导出静态看板快照
"""
from __future__ import annotations

import argparse
import html
import json
import logging
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sources_ext          # noqa: E402  扩展源注册表（顶层不反向依赖 collector）
import textmatch            # noqa: E402  精确 / 受控模糊话题匹配

requests.packages.urllib3.disable_warnings()

# ---------------------------------------------------------------- 路径与常量
ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "database" / "workstation.db"
SETTINGS_PATH = ROOT / "config" / "settings.json"
LOG_DIR = ROOT / "logs"
TZ = timezone(timedelta(hours=8))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# 超过这个秒数没采到新数据即视为「停滞」：看板红字告警、请求触发自愈。
# 采集周期 2 分钟，10 分钟阈值相当于连续 5 轮缺失，既不误报也不放过真断。
FRESHNESS_SECONDS = 600

# 原始采集数据沿用任务书分层目录落盘
RAW_DIRS = {name: ROOT / rel for name, rel in sources_ext.RAW_DIRS.items()}

DEFAULT_SETTINGS = {
    "interval_seconds": 120,
    "request": {"timeout": 12, "retries": 3, "min_gap_per_host": 1.0},
    "retention": {"trends_days": 7, "snapshots_days": 30,
                  "runs_days": 30, "keyword_days": 90},
    "sources": {
        "douyin_hot":         {"enabled": True, "min_interval_seconds": 120},
        "douyin_ies":         {"enabled": True, "min_interval_seconds": 120},
        "weibo_hot":          {"enabled": True, "min_interval_seconds": 120},
        "baidu_hot":          {"enabled": True, "min_interval_seconds": 120},
        "toutiao_hot":        {"enabled": True, "min_interval_seconds": 120, "limit": 50},
        "netease_hot":        {"enabled": True, "min_interval_seconds": 120, "limit": 50},
        "zhihu_search":       {"enabled": True, "min_interval_seconds": 120},
        "bili_square":        {"enabled": True, "min_interval_seconds": 120},
        "bili_rank":          {"enabled": True, "min_interval_seconds": 300},
        "bili_popular":       {"enabled": True, "min_interval_seconds": 300, "pages": 3},
        "bili_search":        {"enabled": True, "min_interval_seconds": 600,
                               "max_keywords": 3, "keywords": [], "per_keyword_sleep": 3},
        "bili_rank_vertical": {"enabled": True, "min_interval_seconds": 300,
                               "per_rid_limit": 20},
        "baidu_vertical":     {"enabled": True, "min_interval_seconds": 3600,
                               "per_tab_limit": 30},
        "bili_precious":      {"enabled": True, "min_interval_seconds": 3600, "limit": 60},
    },
    "derive": {"content_analysis": True, "topic_opportunities": True},
}


def setup_logging():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        fh = logging.FileHandler(LOG_DIR / "collector.log", encoding="utf-8")
        fh.setFormatter(fmt)
        root.addHandler(sh)
        root.addHandler(fh)
    return logging.getLogger("collector")


log = setup_logging()


# ---------------------------------------------------------------- 基础工具
def now_dt() -> datetime:
    return datetime.now(TZ)


def iso(seconds: bool = True) -> str:
    return now_dt().isoformat(timespec="seconds" if seconds else "microseconds")


def stamp() -> str:
    return now_dt().strftime("%Y%m%dT%H%M%S")


def from_unix(ts) -> str | None:
    try:
        return datetime.fromtimestamp(int(ts), TZ).isoformat(timespec="seconds")
    except Exception:
        return None


def jdump(obj) -> str:
    return json.dumps(obj, ensure_ascii=False)


def _deep_merge(base: dict, over: dict) -> dict:
    """递归合并配置：settings.json 只需写要覆盖的字段，新增源自动继承默认值。
    原浅合并会把整个 sources 子字典替换掉，导致缺省参数丢失，这里修掉。"""
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def load_settings() -> dict:
    if SETTINGS_PATH.exists():
        try:
            cfg = json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
            return _deep_merge(DEFAULT_SETTINGS, cfg)
        except Exception as e:
            log.warning(f"读取 settings.json 失败，使用默认配置: {e}")
    return json.loads(jdump(DEFAULT_SETTINGS))


class Http:
    """带重试 + 每域名限速的公开接口会话；不带任何登录态。"""

    def __init__(self, timeout=12, retries=3, min_gap=1.0):
        self.timeout = timeout
        self.retries = retries
        self.min_gap = min_gap
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
        self._last: dict[str, float] = {}

    def _throttle(self, url: str):
        m = re.match(r"https?://([^/]+)", url)
        host = m.group(1) if m else url
        wait = self.min_gap - (time.time() - self._last.get(host, 0))
        if wait > 0:
            time.sleep(wait)
        self._last[host] = time.time()

    def get(self, url: str, referer: str, want: str = "json",
            extra: dict | None = None):
        for att in range(self.retries):
            self._throttle(url)
            h = {"Referer": referer}
            if extra:
                h.update(extra)
            try:
                r = self.s.get(url, headers=h,
                               timeout=self.timeout, verify=False)
                if r.status_code != 200:
                    raise RuntimeError(f"HTTP {r.status_code}")
                if want == "json":
                    return r.json()
                if not r.encoding or r.encoding.lower() == "iso-8859-1":
                    r.encoding = r.apparent_encoding or "utf-8"
                return r.text
            except Exception as e:
                if att == self.retries - 1:
                    log.warning(f"请求失败 {url[:90]} -> {type(e).__name__}: {e}")
                    return None
                # 412 是 B站 WAF 限频（审计 A9：54 轮出现 16 次），
                # 短重试容易连续撞墙，这里给它更长的退避。
                delay = 3.0 * (att + 1) if "412" in str(e) else 1.2 * (att + 1)
                time.sleep(delay)
        return None

    def get_code0(self, url: str, referer: str, tries: int = 4, backoff: float = 2.0,
                  extra: dict | None = None):
        """B站接口专用：HTTP 200 但 code != 0（如 -352 风控 / -799 限频）时退避重试。
        风控响应是「成功状态 + 错误 code」，普通 HTTP 重试不会触发，必须单独处理。"""
        last = None
        for i in range(tries):
            j = self.get(url, referer, extra=extra)
            last = j
            if isinstance(j, dict) and j.get("code") == 0:
                return j
            if i < tries - 1:
                time.sleep(backoff * (i + 1))
        return last


# ---------------------------------------------------------------- 数据源适配器
# 统一返回：{"source", "url", "ok", "note", "items": [标准化记录]}
# 标准化记录两种 kind：
#   work  —— 含 creator / work / metrics / raw
#   trend —— 含 platform / trend_source / rank / keyword / heat_value / tag / extra / raw

def _bili_work(v: dict, src: str) -> dict:
    owner = v.get("owner") or {}
    st = v.get("stat") or {}
    bvid = v.get("bvid")
    return {
        "kind": "work", "platform": "bilibili", "src": src,
        "creator": {
            "platform_uid": str(owner.get("mid") or ""),
            "display_name": owner.get("name"),
            "avatar_url": owner.get("face"),
        },
        "work": {
            "work_id": bvid,
            "title": v.get("title"),
            "url": f"https://www.bilibili.com/video/{bvid}" if bvid else v.get("short_link_v2"),
            "cover_url": v.get("pic"),
            "duration": v.get("duration"),
            "work_type": "video",
            "publish_time": from_unix(v.get("pubdate")),
        },
        "metrics": {
            "play_count": st.get("view"), "like_count": st.get("like"),
            "comment_count": st.get("reply"), "share_count": st.get("share"),
            "collect_count": st.get("favorite"),
            "extra": {
                "danmaku": st.get("danmaku"), "coin": st.get("coin"),
                "score": v.get("score"), "his_rank": st.get("his_rank"),
                "now_rank": st.get("now_rank"), "vt": st.get("vt"),
                "category": v.get("tname"), "category_v2": v.get("tnamev2"),
            },
        },
        "raw": v,
    }


def _dur(x) -> int | None:
    if isinstance(x, int):
        return x
    if isinstance(x, str) and ":" in x:
        try:
            p = [int(i) for i in x.split(":")]
            if len(p) == 2:
                return p[0] * 60 + p[1]
            if len(p) == 3:
                return p[0] * 3600 + p[1] * 60 + p[2]
        except Exception:
            return None
    return None


def _bili_search_work(v: dict, kw: str) -> dict:
    bvid = v.get("bvid")
    pic = v.get("pic") or ""
    if pic.startswith("//"):
        pic = "https:" + pic
    return {
        "kind": "work", "platform": "bilibili", "src": "bili_search",
        "creator": {
            "platform_uid": str(v.get("mid") or ""),
            "display_name": v.get("author"),
        },
        "work": {
            "work_id": bvid,
            "title": re.sub(r"<[^>]+>", "", v.get("title") or ""),
            "url": v.get("arcurl") or (f"https://www.bilibili.com/video/{bvid}" if bvid else None),
            "cover_url": pic or None,
            "duration": _dur(v.get("duration")),
            "work_type": "video",
            "publish_time": from_unix(v.get("pubdate")),
        },
        "metrics": {
            "play_count": v.get("play"), "like_count": v.get("like"),
            "comment_count": v.get("review"), "share_count": None,
            "collect_count": v.get("favorites"),
            "extra": {
                "keyword": kw, "tag": v.get("tag"),
                "description": v.get("description"),
                "danmaku": v.get("video_review"),
            },
        },
        "raw": v,
    }


def a_douyin_hot(http: Http, cfg: dict, seed: list) -> dict:
    url = ("https://www.douyin.com/aweme/v1/web/hot/search/list/"
           "?device_platform=webapp&aid=6383&channel=channel_pc_web&detail_list=1")
    j = http.get(url, "https://www.douyin.com/hot")
    if not isinstance(j, dict):
        return {"source": "douyin_hot", "url": url, "ok": False,
                "note": "响应非 JSON 或请求失败", "items": []}
    wl = (j.get("data") or {}).get("word_list") or []
    if not wl:
        return {"source": "douyin_hot", "url": url, "ok": False,
                "note": f"word_list 为空 (status_code={j.get('status_code')})", "items": []}
    label_map = {0: "", 1: "新", 2: "荐", 3: "热", 4: "爆", 5: "首", 6: "沸"}
    items = []
    for w in wl:
        items.append({
            "kind": "trend", "platform": "douyin", "trend_source": "douyin_hot",
            "rank": w.get("position"),
            "keyword": w.get("word") or w.get("sentence_id") or "",
            "heat_value": w.get("hot_value"),
            "tag": label_map.get(w.get("label"), str(w.get("label") or "")),
            "extra": {
                "group_id": w.get("group_id"), "sentence_id": w.get("sentence_id"),
                "video_count": w.get("video_count"),
                "discuss_video_count": w.get("discuss_video_count"),
                "event_time": w.get("event_time"), "max_rank": w.get("max_rank"),
                "label_raw": w.get("label"),
            },
            "raw": w,
        })
    return {"source": "douyin_hot", "url": url, "ok": True,
            "note": f"{len(items)} 条热点词", "items": items}


def a_weibo_hot(http: Http, cfg: dict, seed: list) -> dict:
    url = "https://weibo.com/ajax/side/hotSearch"
    j = http.get(url, "https://weibo.com/")
    rt = ((j or {}).get("data") or {}).get("realtime") or [] if isinstance(j, dict) else []
    items = []
    for i, w in enumerate(rt):
        items.append({
            "kind": "trend", "platform": "weibo", "trend_source": "weibo_hot",
            "rank": i + 1,  # 用序号保证 UNIQUE(platform,trend_source,trend_time,rank) 不冲突
            "keyword": w.get("word") or w.get("note") or "",
            "heat_value": w.get("num"),
            "tag": w.get("label_name") or w.get("icon_desc") or "",
            "extra": {
                "realpos": w.get("realpos"), "rank_raw": w.get("rank"),
                "flag_desc": w.get("flag_desc"), "note": w.get("note"),
                "word_scheme": w.get("word_scheme"), "topic_flag": w.get("topic_flag"),
            },
            "raw": w,
        })
    return {"source": "weibo_hot", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条热搜" if items else "响应为空", "items": items}


def a_baidu_hot(http: Http, cfg: dict, seed: list) -> dict:
    url = "https://top.baidu.com/board?tab=realtime"
    t = http.get(url, "https://www.baidu.com/", want="text")
    items = []
    note = ""
    if isinstance(t, str) and t:
        m = re.search(r"<!--s-data:(.*?)-->", t, re.S)
        if m:
            try:
                d = json.loads(html.unescape(m.group(1)))
                for card in d.get("data", {}).get("cards", []):
                    for c in card.get("content", []):
                        try:
                            heat = int(c.get("hotScore") or 0)
                        except Exception:
                            heat = 0
                        items.append({
                            "kind": "trend", "platform": "baidu",
                            "trend_source": "baidu_realtime",
                            "rank": len(items) + 1,
                            "keyword": c.get("word") or c.get("query") or "",
                            "heat_value": heat,
                            "tag": c.get("hotChange") or c.get("hotTag") or "",
                            "extra": {
                                "desc": c.get("desc"), "img": c.get("img"),
                                "url": c.get("url") or c.get("appUrl"),
                                "hotTag": c.get("hotTag"),
                            },
                            "raw": c,
                        })
            except Exception as e:
                note = f"s-data 解析失败: {type(e).__name__}: {e}"
        else:
            note = "未找到 s-data 注入块"
    else:
        note = "响应为空或非文本"
    return {"source": "baidu_hot", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条热搜" if items else note, "items": items}


def a_bili_rank(http: Http, cfg: dict, seed: list) -> dict:
    # 实测：裸 Referer 会被风控返回 code=-352；用排行榜页精确 Referer + Origin 可稳定拿到 100 条。
    ref = "https://www.bilibili.com/v/popular/rank/all"
    hdr = {"Origin": "https://www.bilibili.com"}
    url = "https://api.bilibili.com/x/web-interface/ranking/v2?rid=0&type=all"
    j = http.get_code0(url, ref, extra=hdr)
    lst = ((j or {}).get("data") or {}).get("list") or [] if isinstance(j, dict) else []
    used, degraded = url, False
    if not lst:
        # 兜底：旧版排行榜接口（实测同样稳定返回 100 条）
        url2 = "https://api.bilibili.com/x/web-interface/ranking?rid=0&type=all"
        j = http.get_code0(url2, ref, extra=hdr)
        lst = ((j or {}).get("data") or {}).get("list") or [] if isinstance(j, dict) else []
        used, degraded = url2, bool(lst)
    items = [_bili_work(v, "bili_rank") for v in lst]
    note = (f"{len(items)} 条上榜作品" if items
            else "v2 与旧版接口均被风控(code=%s)" % (j or {}).get("code"))
    if degraded:
        note += "（旧版接口降级）"
    return {"source": "bili_rank", "url": used, "ok": bool(items),
            "note": note, "items": items}


def a_bili_popular(http: Http, cfg: dict, seed: list) -> dict:
    pages = int(cfg.get("pages", 3))
    items, last_url = [], ""
    for pn in range(1, pages + 1):
        url = f"https://api.bilibili.com/x/web-interface/popular?ps=20&pn={pn}"
        j = http.get_code0(url, "https://www.bilibili.com/")
        lst = ((j or {}).get("data") or {}).get("list") or [] if isinstance(j, dict) else []
        items.extend(_bili_work(v, "bili_popular") for v in lst)
        last_url = url
        if not lst:
            break
    return {"source": "bili_popular", "url": last_url, "ok": bool(items),
            "note": f"{len(items)} 条热门作品 / {pages} 页", "items": items}


def a_bili_search(http: Http, cfg: dict, seed: list) -> dict:
    pool = [k for k in (cfg.get("keywords") or []) if k] or [k for k in seed if k]
    pool = list(dict.fromkeys(pool))            # 去重保序
    max_kw = int(cfg.get("max_keywords", 3))
    if len(pool) > max_kw:
        # 按采集周期轮换种子：原逻辑永远取前 3 个词，导致搜索到的作品集合
        # 长期不变、works 主表停止增长。轮换后每轮覆盖不同热点话题。
        step = max(1, int(cfg.get("min_interval_seconds", 600)))
        off = (int(time.time()) // step) % len(pool)
        kws = [pool[(off + i) % len(pool)] for i in range(max_kw)]
    else:
        kws = pool[:max_kw]
    gap = float(cfg.get("per_keyword_sleep", 3))
    items, used = [], []
    for i, kw in enumerate(kws):
        url = ("https://api.bilibili.com/x/web-interface/search/type"
               f"?search_type=video&keyword={quote(str(kw))}&page=1")
        j = http.get(url, "https://search.bilibili.com/")
        res = ((j or {}).get("data") or {}).get("result") or [] if isinstance(j, dict) else []
        items.extend(_bili_search_work(v, kw) for v in res[:10])
        used.append(kw)
        if i < len(kws) - 1:
            time.sleep(gap)  # 搜索接口风控较严，主动限速
    return {"source": "bili_search",
            "url": "https://search.bilibili.com/", "ok": bool(items),
            "note": f"关键词 {used} 命中 {len(items)} 条作品" if items else f"关键词 {used} 未取到结果(可能被限频，已隔离)",
            "items": items}


# 扩展源在 tools/sources_ext.py 单独维护；这里只做注册合并，
# 避免 collector.py 随着数据源增加继续膨胀（第二轮可读性改造）。
ORDER: list[str] = list(sources_ext.ORDER)
ADAPTERS: dict = {
    "douyin_hot": a_douyin_hot,
    "weibo_hot": a_weibo_hot,
    "baidu_hot": a_baidu_hot,
    "bili_rank": a_bili_rank,
    "bili_popular": a_bili_popular,
    "bili_search": a_bili_search,
    **sources_ext.ADAPTERS,
}
SOURCE_LABELS: dict[str, str] = {**sources_ext.LABELS}
SOURCE_META: dict[str, dict] = sources_ext.SOURCE_META

# 每个平台在「同轮热榜对比 / 简报 / 智库沉淀」中展示的主源。
# 抖音有两个独立源（主榜 + 镜像校验）、百度有实时榜 + 多个垂直分榜，
# 若不指定主源会把同一平台的多份榜混在一列里。两套代码（dashboard/knowledge）
# 都从这里取，避免漂移。
# PRIMARY_SOURCES 是所有「关键词型」主源的合集，直接决定智库沉淀（knowledge.incubate）
# 从哪些源抽取话题。已把百度 4 个垂直分榜（财经/汽车/旅游/科技）纳入，
# 它们是单人自拍口播最对口、最易做出个人 IP 的选题燃料。
PRIMARY_SOURCE: dict[str, str] = {
    "douyin": "douyin_hot", "weibo": "weibo_hot", "baidu": "baidu_realtime",
    "bilibili": "bili_square", "toutiao": "toutiao_board",
    "netease": "netease_news", "zhihu": "zhihu_top_search",
}
PRIMARY_SOURCES: tuple[str, ...] = (
    "douyin_hot", "weibo_hot", "baidu_realtime",
    "baidu_finance", "baidu_car", "baidu_travel", "baidu_technology",
    "bili_square", "toutiao_board", "netease_news", "zhihu_top_search",
)


# ---------------------------------------------------------------- 入库
def connect() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    c.execute("PRAGMA journal_mode = WAL")
    return c


KEYWORDS_PATH = ROOT / "config" / "keywords.json"


def ensure_extra_tables(conn):
    """幂等建立扩展表，并从 config/keywords.json 同步关注词。
    不改动原有 8 表，用 IF NOT EXISTS 保证已有库平滑升级。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tracked_keywords (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword      TEXT NOT NULL UNIQUE,
            category     TEXT,
            enabled      INTEGER DEFAULT 1,
            created_at   TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS keyword_snapshots (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            keyword_id    INTEGER NOT NULL,
            round_at      TEXT NOT NULL,
            hits          INTEGER DEFAULT 0,
            platforms     TEXT,
            best_rank     INTEGER,
            total_heat    INTEGER,
            matched_words TEXT,
            data_status   TEXT DEFAULT 'derived',
            FOREIGN KEY (keyword_id) REFERENCES tracked_keywords(id) ON DELETE CASCADE,
            UNIQUE(keyword_id, round_at)
        )
    """)

    # 数据源目录：把「有哪些源、各自多久采一次、提供什么、合规口径」写进库，
    # 看板与审计脚本直接查库，不再靠读代码猜。
    conn.execute("""
        CREATE TABLE IF NOT EXISTS source_catalog (
            name        TEXT PRIMARY KEY,
            label       TEXT,
            platform    TEXT,
            tier        TEXT,
            interval_s  INTEGER,
            provides    TEXT,
            enabled     INTEGER DEFAULT 1,
            compliance  TEXT DEFAULT '免登录公开接口；无登录态、无签名、无验证码绕过',
            updated_at  TEXT
        )
    """)

    # 部分平台不公开热度（网易新闻 / 知乎热搜）。用独立标记区分
    # 「真实为 0」与「平台不提供」，避免质量自检把设计口径误判成缺陷。
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(trends)")}
    if "heat_available" not in cols:
        conn.execute("ALTER TABLE trends ADD COLUMN heat_available INTEGER DEFAULT 1")

    # collection_runs 原 items_collected 记的是「处理了多少条」，跨源重复作品
    # 被 INSERT OR IGNORE 掉也照记，审计 A2 实测虚报 17/60。这里补 items_seen
    # 区分「看到」与「真正新增」，items_collected 今后只记真实新增。
    run_cols = {r["name"] for r in conn.execute("PRAGMA table_info(collection_runs)")}
    if "items_seen" not in run_cols:
        conn.execute("ALTER TABLE collection_runs ADD COLUMN items_seen INTEGER DEFAULT 0")

    # 口径自愈（幂等）：heat_value 为空 = 平台不公开热度，绝不能被标成
    # 「本应提供」。代码热更新中间态曾留下 60 条脏标记（13:55 轮次），
    # 这条 UPDATE 每轮兜底修正，避免质量自检长期误报。
    conn.execute("""UPDATE trends SET heat_available = 0
                    WHERE heat_value IS NULL AND heat_available <> 0""")

    # 高频看板查询 / 分级节流查询 / 数据保留清理所需的索引（原先只有 6 个，
    # growth 与 continuity 都在全表扫描，数据量涨上去会拖慢每 30s 的刷新）。
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_ws_snap_at ON work_snapshots(snapshot_at)",
        "CREATE INDEX IF NOT EXISTS idx_cs_snap_at ON creator_snapshots(snapshot_at)",
        "CREATE INDEX IF NOT EXISTS idx_trends_time_plat ON trends(trend_time, platform)",
        "CREATE INDEX IF NOT EXISTS idx_trends_src_time ON trends(trend_source, trend_time)",
        "CREATE INDEX IF NOT EXISTS idx_runs_source_at ON collection_runs(source, run_at)",
        "CREATE INDEX IF NOT EXISTS idx_runs_at ON collection_runs(run_at)",
        "CREATE INDEX IF NOT EXISTS idx_kw_round ON keyword_snapshots(round_at)",
    ]:
        conn.execute(sql)

    now = iso()
    enabled_map = {k: (1 if v.get("enabled", True) else 0)
                   for k, v in load_settings().get("sources", {}).items()}
    for name, meta in SOURCE_META.items():
        conn.execute("""
            INSERT INTO source_catalog
              (name, label, platform, tier, interval_s, provides, enabled, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
              label=excluded.label, platform=excluded.platform,
              tier=excluded.tier, interval_s=excluded.interval_s,
              provides=excluded.provides, enabled=excluded.enabled,
              updated_at=excluded.updated_at
        """, (name, SOURCE_LABELS.get(name), meta.get("platform"),
              meta.get("tier"), meta.get("interval"), meta.get("provides"),
              enabled_map.get(name, 1), now))

    if KEYWORDS_PATH.exists():
        try:
            cfg = json.loads(KEYWORDS_PATH.read_text(encoding="utf-8"))
            for item in cfg.get("keywords", []):
                kw = (item.get("keyword") or "").strip()
                if not kw:
                    continue
                conn.execute("""
                    INSERT OR IGNORE INTO tracked_keywords
                      (keyword, category, enabled, created_at)
                    VALUES (?, ?, 1, ?)
                """, (kw, item.get("category"), now))
        except Exception as e:
            log.warning(f"读取关注词配置失败: {e}")
    conn.commit()


def derive_keyword_tracking(conn, round_ts: str) -> int:
    """每轮把关注词在四平台热榜的命中情况落成时序快照。
    命中 0 也写入一条（hits=0），保证时间序列完整、可看「哪天没上热榜」。"""
    kws = conn.execute(
        "SELECT id, keyword FROM tracked_keywords WHERE enabled = 1").fetchall()
    if not kws:
        return 0
    trends = conn.execute("""
        SELECT platform, rank, keyword, heat_value
        FROM trends WHERE trend_time = ?
    """, (round_ts,)).fetchall()
    n = 0
    for k in kws:
        kw = k["keyword"]
        matched = [t for t in trends
                   if textmatch.keyword_hit(kw, t["keyword"])]
        plats = sorted({t["platform"] for t in matched})
        best = min([t["rank"] for t in matched if t["rank"]], default=None)
        heat = sum(t["heat_value"] or 0 for t in matched)
        words = [{"platform": t["platform"], "word": t["keyword"],
                  "rank": t["rank"], "heat": t["heat_value"]} for t in matched]
        cur = conn.execute("""
            INSERT OR IGNORE INTO keyword_snapshots
              (keyword_id, round_at, hits, platforms, best_rank,
               total_heat, matched_words, data_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'derived')
        """, (k["id"], round_ts, len(plats), jdump(plats), best, heat,
              jdump(words)))
        n += cur.rowcount
    return n


def ingest_work(conn, it: dict, round_ts: str, source: str, src_url: str) -> int:
    cr, w, m = it["creator"], it["work"], it["metrics"]
    uid, wid_key = cr.get("platform_uid"), w.get("work_id")
    if not uid or not wid_key:
        return 0
    conn.execute("""
        INSERT INTO creators
          (platform, platform_uid, handle, display_name, avatar_url,
           first_seen, last_seen, source, source_url, collected_at, _raw_json)
        VALUES ('bilibili', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(platform, platform_uid) DO UPDATE SET
          display_name = excluded.display_name,
          avatar_url   = excluded.avatar_url,
          last_seen    = excluded.last_seen,
          _raw_json    = excluded._raw_json
    """, (uid, None, cr.get("display_name"), cr.get("avatar_url"),
          round_ts, round_ts, source, src_url, round_ts, jdump(cr)))
    cid = conn.execute(
        "SELECT id FROM creators WHERE platform='bilibili' AND platform_uid=?",
        (uid,)).fetchone()[0]

    conn.execute("""
        INSERT INTO works
          (platform, work_id, creator_id, title, publish_time, url, cover_url,
           duration, work_type, first_seen, last_seen, source, source_url,
           collected_at, _raw_json)
        VALUES ('bilibili', ?, ?, ?, ?, ?, ?, ?, 'video', ?, ?, ?, ?, ?, ?)
        ON CONFLICT(platform, work_id) DO UPDATE SET
          title       = excluded.title,
          last_seen   = excluded.last_seen,
          creator_id  = excluded.creator_id,
          _raw_json   = excluded._raw_json
    """, (wid_key, cid, w.get("title"), w.get("publish_time"), w.get("url"),
          w.get("cover_url"), w.get("duration"), round_ts, round_ts,
          source, src_url, round_ts, jdump(it["raw"])))
    row = conn.execute(
        "SELECT id FROM works WHERE platform='bilibili' AND work_id=?",
        (wid_key,)).fetchone()[0]

    # 返回真实新增的指标快照数，而非恒定 1：同一作品被多个榜单重复采到时
    # INSERT OR IGNORE 会忽略，旧逻辑仍计数，导致 collection_runs 虚报（审计 A2）。
    cur = conn.execute("""
        INSERT OR IGNORE INTO work_snapshots
          (work_id, snapshot_at, play_count, like_count, comment_count,
           share_count, collect_count, raw_metrics_json, source, source_url, collected_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (row, round_ts, m.get("play_count"), m.get("like_count"),
          m.get("comment_count"), m.get("share_count"), m.get("collect_count"),
          jdump(m.get("extra") or {}), source, src_url, round_ts))
    return 1 if cur.rowcount else 0


def ingest_trend(conn, it: dict, round_ts: str, source: str, src_url: str) -> int:
    if not it.get("keyword"):
        return 0
    cur = conn.execute("""
        INSERT OR IGNORE INTO trends
          (platform, trend_source, rank, keyword, heat_value, heat_available,
           tag, trend_time, source, source_url, collected_at, _raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (it["platform"], it["trend_source"], it.get("rank"), it["keyword"],
          it.get("heat_value"), 1 if it.get("heat_value") is not None else 0,
          it.get("tag"), round_ts, source, src_url,
          round_ts, jdump({"extra": it.get("extra") or {}, "raw": it.get("raw")})))
    return cur.rowcount


def ingest_creator_snapshots(conn, round_ts: str, src_url: str | None = None) -> int:
    """在所有作品源入库**之后**统一调用一次，按本轮全部作品快照聚合创作者快照。

    审计 A1 发现的原缺陷：原实现挂在 ingest() 里，每个含 work 的源都会调用一次，
    而 SQL 按 snapshot_at 聚合「当时已入库」的快照 + INSERT OR IGNORE，
    导致同一轮的创作者指标被切成三段（bili_rank 4894 / popular 2225 / search 1278），
    每个 UP 主的 total_views、work_count 只统计到首次插入时的部分作品，
    看板「创作者榜」排序系统性失真。
    修复：末尾统一算、先删本轮旧值再全量重算，source 统一标 'aggregate'。

    语义：该 UP 主在本轮各榜单中上榜作品的汇总表现；非粉丝数——公开榜单
    不提供 follower_count，故留空，绝不用估算值填充。
    """
    conn.execute("DELETE FROM creator_snapshots WHERE snapshot_at = ?", (round_ts,))
    rows = conn.execute("""
        SELECT w.creator_id AS cid,
               COUNT(*)              AS work_count,
               SUM(s.play_count)     AS total_views,
               SUM(s.like_count)     AS total_likes,
               AVG(s.play_count)     AS avg_views,
               SUM(s.comment_count)  AS total_comments,
               SUM(s.collect_count)  AS total_favs
        FROM work_snapshots s
        JOIN works w ON w.id = s.work_id
        WHERE s.snapshot_at = ? AND w.platform = 'bilibili'
              AND w.creator_id IS NOT NULL
        GROUP BY w.creator_id
    """, (round_ts,)).fetchall()
    n = 0
    for r in rows:
        metrics = {
            "basis": "本轮上榜作品聚合（非账号粉丝数）",
            "source_scope": "本轮全部作品源（统一聚合，不再分片）",
            "total_views": r["total_views"], "total_likes": r["total_likes"],
            "total_comments": r["total_comments"], "total_favs": r["total_favs"],
            "avg_views": r["avg_views"], "work_count": r["work_count"],
        }
        cur = conn.execute("""
            INSERT INTO creator_snapshots
              (creator_id, snapshot_at, work_count, total_likes, avg_views,
               raw_metrics_json, source, source_url, collected_at)
            VALUES (?, ?, ?, ?, ?, ?, 'aggregate', ?, ?)
        """, (r["cid"], round_ts, r["work_count"], r["total_likes"],
              r["avg_views"], jdump(metrics), src_url, round_ts))
        n += cur.rowcount
    return n


def ingest(conn, data: dict, round_ts: str) -> dict:
    """返回 {'new','seen','has_work'}：
      new —— 真实新增的快照/趋势条数（写入 collection_runs.items_collected）
      seen —— 本轮看到的条目数（去重前，写入 items_seen 供审计核对）
      has_work —— 本轮是否有作品类数据（决定是否重算创作者聚合）
    创作者聚合刻意不在这里做，见 ingest_creator_snapshots 的说明。"""
    source, src_url = data.get("source"), data.get("url")
    new, seen, has_work = 0, 0, False
    for it in data.get("items", []):
        seen += 1
        try:
            if it.get("kind") == "work":
                new += ingest_work(conn, it, round_ts, source, src_url)
                has_work = True
            elif it.get("kind") == "trend":
                new += ingest_trend(conn, it, round_ts, source, src_url)
        except Exception as e:
            log.warning(f"单条入库失败 [{source}]: {type(e).__name__}: {e}")
    return {"new": new, "seen": seen, "has_work": has_work}


def save_raw(name: str, round_ts: str, data: dict):
    d = RAW_DIRS.get(name, ROOT / "03_作品数据库" / "raw")
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{name}_{stamp()}.json"
    p.write_text(jdump({
        "source": name, "label": SOURCE_LABELS.get(name),
        "url": data.get("url"), "collected_at": round_ts,
        "ok": data.get("ok"), "note": data.get("note"),
        "items": data.get("items", []),
    }), encoding="utf-8")
    return p


# ---------------------------------------------------------------- 派生层（诚实标注 derived / 规则引擎）
def _band(duration) -> str:
    if not duration:
        return "未知"
    if duration < 60:
        return "<1分钟"
    if duration < 180:
        return "1-3分钟"
    if duration < 600:
        return "3-10分钟"
    if duration < 1800:
        return "10-30分钟"
    return ">30分钟"


def derive_content_analysis(conn, round_ts: str) -> int:
    rows = conn.execute("""
        SELECT w.id, w.title, w.duration, s.play_count, s.like_count,
               s.comment_count, s.collect_count, s.raw_metrics_json
        FROM works w
        JOIN work_snapshots s ON s.work_id = w.id
        WHERE s.snapshot_at = ?
          AND w.id NOT IN (SELECT work_id FROM content_analysis)
    """, (round_ts,)).fetchall()
    n = 0
    for r in rows:
        title = r["title"] or ""
        extra = {}
        try:
            extra = json.loads(r["raw_metrics_json"] or "{}")
        except Exception:
            pass
        play = r["play_count"] or 0
        feat = {
            "title_length": len(title),
            "has_number": bool(re.search(r"\d", title)),
            "has_question": bool(re.search(r"[?？]", title)),
            "has_quote": bool(re.search(r"[「」“”\"《》【】]", title)),
            "duration_band": _band(r["duration"]),
            "category": extra.get("category"),
            "rank_score": extra.get("score"),
            "engagement_rate": round(
                ((r["like_count"] or 0) + (r["comment_count"] or 0)
                 + (r["collect_count"] or 0)) / max(play, 1), 4),
            "generator": "rule_engine_v1",
            "disclaimer": "规则化速览标签，非人工/模型深度拆解",
        }
        bits = []
        if feat["has_number"]:
            bits.append("含数字")
        if feat["has_question"]:
            bits.append("设问式")
        if feat["has_quote"]:
            bits.append("引号/书名号强调")
        bits.append(f"标题{feat['title_length']}字")
        bits.append(f"时长{feat['duration_band']}")
        cur = conn.execute("""
            INSERT OR IGNORE INTO content_analysis
              (work_id, analysis_type, topic, title_eval, hook, structure,
               core_view, argumentation, emotion, narrative, raw_analysis_json,
               analyst, confidence, analysis_at, source, source_url,
               collected_at, data_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (r["id"], "quick_tag", (title[:30] or extra.get("category")),
              "；".join(bits) + "（规则速览）", None, None, None, None, None, None,
              jdump(feat), "rule_engine_v1", "low", round_ts,
              "bili_rank/bili_popular", None, round_ts, "derived"))
        n += cur.rowcount
    return n


def derive_topic_opportunities(conn, round_ts: str) -> int:
    """候选选题：跨平台共振（精确 + 受控模糊）或抖音主榜前 10。

    审计 A3 修复：原按「整句归一化完全相等」聚合，末轮 152 条只命中 5 组且
    抖音 0 参与。改用 textmatch.resonance 的 shingle 共现后，措辞不同的同一
    事件也能关联；模糊簇一律降级 priority 并在文案里写明「需人工核实」。
    """
    rows = [dict(r) for r in conn.execute(
        "SELECT id, platform, trend_source, keyword, heat_value, rank "
        "FROM trends WHERE trend_time = ?", (round_ts,)).fetchall()]
    clusters = textmatch.resonance(rows, min_platforms=2, max_clusters=40)

    cands = []
    for g in clusters:
        cands.append({
            "title": g["keyword"], "plats": g["platforms"],
            "heat": g["max_heat"], "ids": sorted({m["id"] for m in g["members"] if m.get("id")}),
            "match": g["match"], "evidence": g["evidence"],
            "best_rank": g["best_rank"], "origin": "resonance",
        })
    # 抖音是主方向：主榜前 10 即使没有跨平台共振也值得列为候选
    seen = {textmatch.norm(c["title"]) for c in cands}
    for r in rows:
        if (r["platform"] == "douyin" and r["trend_source"] == "douyin_hot"
                and (r["rank"] or 999) <= 10
                and textmatch.norm(r["keyword"]) not in seen and r["keyword"]):
            seen.add(textmatch.norm(r["keyword"]))
            cands.append({
                "title": r["keyword"], "plats": ["douyin"],
                "heat": r["heat_value"] or 0, "ids": [r["id"]],
                "match": "exact", "evidence": [], "best_rank": r["rank"],
                "origin": "douyin_top10",
            })

    n = 0
    for g in cands[:60]:
        plats = g["plats"]
        base = "P0" if len(plats) >= 3 else ("P1" if len(plats) == 2 else "P2")
        # 模糊匹配是推断，降级一级，避免和已确证的事实同权推荐
        order = {"P0": "P1", "P1": "P2", "P2": "P2"}
        prio = order[base] if g["match"] == "fuzzy" else base
        dup = conn.execute(
            "SELECT 1 FROM topic_opportunities WHERE title = ? AND created_at LIKE ?",
            (g["title"], round_ts[:10] + "%")).fetchone()
        if dup:
            continue
        ev = ("；共现片段 " + "/".join(g["evidence"])) if g["evidence"] else ""
        match_note = ("【模糊推断，需人工核实】" if g["match"] == "fuzzy"
                      else "【整句精确共振】")
        payload = {
            "platforms": plats, "platform_count": len(plats),
            "total_heat": g["heat"], "best_rank": g["best_rank"],
            "match": g["match"], "evidence": g["evidence"],
            "origin": g["origin"], "generator": "rule_engine_v2_textmatch",
            "disclaimer": "由跨平台热榜规则聚合生成；模糊匹配为推断，需人工二次筛选",
        }
        cur = conn.execute("""
            INSERT INTO topic_opportunities
              (title, based_on, need_ref, trend_ref, supply_analysis,
               differentiation, personal_fit, series_potential, priority, status,
               raw_json, planner, created_at, source, source_url,
               collected_at, data_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (g["title"], "trend", None, jdump(g["ids"]),
              f"{match_note}跨平台命中 {len(plats)} 个平台（{'/'.join(plats)}），"
              f"合计热度 {g['heat']}，最佳榜位 {g['best_rank']}{ev}",
              f"可从 {'/'.join(plats)} 多平台视角做差异化切入；"
              f"建议结合个人表达方向二次筛选，事实以原始链接核实",
              None, "待评估", prio, "candidate", jdump(payload),
              "rule_engine_v2_textmatch", round_ts, "trends", None, round_ts, "derived"))
        n += cur.rowcount
    return n


# ---------------------------------------------------------------- 单轮编排
TABLES = ["creators", "creator_snapshots", "works", "work_snapshots",
          "comments", "content_analysis", "trends", "topic_opportunities",
          "collection_runs", "tracked_keywords", "keyword_snapshots"]


def prune_db(cfg: dict | None = None) -> dict:
    """按保留天数清理历史时序明细，防止 2 分钟粒度长期运行把库撑爆。

    只删时间序列明细（trends / 各类 snapshots / 审计轮次），**不删主表**
    works、creators、content_analysis、topic_opportunities —— 它们是稳定的
    主数据与派生结论。看板只需要末轮 + 上一轮，深层历史价值有限。
    """
    cfg = cfg or load_settings()
    r = cfg.get("retention", {})
    conn = connect()
    out = {}
    plan = [
        ("trends", "trend_time", int(r.get("trends_days", 7))),
        ("work_snapshots", "snapshot_at", int(r.get("snapshots_days", 30))),
        ("creator_snapshots", "snapshot_at", int(r.get("snapshots_days", 30))),
        ("collection_runs", "run_at", int(r.get("runs_days", 30))),
        ("keyword_snapshots", "round_at", int(r.get("keyword_days", 90))),
    ]
    for table, col, days in plan:
        cut = (now_dt() - timedelta(days=days)).isoformat(timespec="seconds")
        out[table] = conn.execute(
            f"DELETE FROM {table} WHERE {col} < ?", (cut,)).rowcount
    conn.commit()
    conn.close()
    return out


def run_once(cfg: dict | None = None, http: Http | None = None) -> dict:
    cfg = cfg or load_settings()
    rq = cfg.get("request", {})
    http = http or Http(rq.get("timeout", 12), rq.get("retries", 3),
                        rq.get("min_gap_per_host", 1.0))
    round_ts = iso()
    conn = connect()
    ensure_extra_tables(conn)
    summary = {"round_at": round_ts, "sources": [], "totals": {}}
    seed: list[str] = []
    has_work_round = False
    log.info(f"===== 采集轮次开始 {round_ts} | 分级调度 =====")

    for name in ORDER:
        scfg = cfg.get("sources", {}).get(name, {})
        if not scfg.get("enabled", True):
            continue
        meta = SOURCE_META.get(name, {})

        # --- 分级节流：未到该源采集间隔就写一条审计记录后跳过 ---
        min_gap = int(scfg.get("min_interval_seconds") or meta.get("interval") or 0)
        if min_gap > 0:
            last = conn.execute(
                "SELECT MAX(run_at) FROM collection_runs "
                "WHERE source = ? AND status IN ('success','partial')",
                (name,)).fetchone()[0]
            age = None
            if last:
                try:
                    age = (now_dt() - datetime.fromisoformat(last)).total_seconds()
                except Exception:
                    age = None
            if age is not None and age < min_gap:
                note = (f"分级调度：距上次成功 {int(age)}s < 间隔 {min_gap}s，"
                        f"本轮跳过（{meta.get('tier', '分级')}）")
                conn.execute("""
                    INSERT INTO collection_runs
                      (run_at, role, source, target, items_collected, items_seen,
                       status, note, source_url)
                    VALUES (?, '数据采集员', ?, ?, 0, 0, 'scheduled_skip', ?, NULL)
                """, (round_ts, name, SOURCE_LABELS.get(name), note))
                conn.commit()
                summary["sources"].append({
                    "source": name, "label": SOURCE_LABELS.get(name),
                    "status": "scheduled_skip", "items": 0, "seen": 0,
                    "elapsed_s": 0, "note": note, "tier": meta.get("tier"),
                })
                log.info(f"[{name}] scheduled_skip {note}")
                continue

        t0 = time.time()
        n, seen, status, note = 0, 0, "blocked", ""
        try:
            data = ADAPTERS[name](http, scfg, seed)
            save_raw(name, round_ts, data)
            st = ingest(conn, data, round_ts)
            n, seen = st["new"], st["seen"]
            has_work_round = has_work_round or st["has_work"]
            note = data.get("note", "")
            # 成功 = 接口正常且确实看到数据；全为跨源重复（new=0）仍算成功，
            # 只是本轮没有新增快照，不再像旧逻辑那样把它误判成 blocked。
            status = ("success" if (data.get("ok") and seen > 0)
                      else "partial" if seen > 0 else "blocked")
            if meta.get("tier") == "快线":
                seed.extend([it["keyword"] for it in data.get("items", [])
                             if it.get("keyword")][:3])
        except Exception as e:
            note = f"{type(e).__name__}: {e}"
            log.exception(f"[{name}] 适配器异常（已隔离）")

        conn.execute("""
            INSERT INTO collection_runs
              (run_at, role, source, target, items_collected, items_seen,
               status, note, source_url)
            VALUES (?, '数据采集员', ?, ?, ?, ?, ?, ?, NULL)
        """, (round_ts, name, SOURCE_LABELS.get(name), n, seen, status,
              (note + f" | 新增 {n} / 处理 {seen}")[:500]))
        conn.commit()  # 每源独立提交，单源失败不回滚整轮
        elapsed = round(time.time() - t0, 1)
        summary["sources"].append({
            "source": name, "label": SOURCE_LABELS.get(name),
            "status": status, "items": n, "seen": seen,
            "elapsed_s": elapsed, "note": note, "tier": meta.get("tier"),
        })
        log.info(f"[{name}] {status} 新增 {n} / 处理 {seen} 条 / {elapsed}s  {note}")

    # --- 创作者聚合：所有作品源入库后统一算一次（审计 A1 修复）---
    if has_work_round:
        try:
            k = ingest_creator_snapshots(conn, round_ts,
                                         "database/workstation.db#aggregate")
            log.info(f"[派生] creator_snapshots 统一重算 {k} 条")
            summary["derive_creator_snapshots"] = k
        except Exception as e:
            log.warning(f"[派生] creator_snapshots 失败: {e}")
    conn.commit()

    # --- 其余派生层（各步独立容错）---
    derive = cfg.get("derive", {})
    if derive.get("content_analysis", True):
        try:
            k = derive_content_analysis(conn, round_ts)
            log.info(f"[派生] content_analysis +{k}")
            summary["derive_content_analysis"] = k
        except Exception as e:
            log.warning(f"[派生] content_analysis 失败: {e}")
    if derive.get("topic_opportunities", True):
        try:
            k = derive_topic_opportunities(conn, round_ts)
            log.info(f"[派生] topic_opportunities +{k}")
            summary["derive_topic_opportunities"] = k
        except Exception as e:
            log.warning(f"[派生] topic_opportunities 失败: {e}")
    try:
        k = derive_keyword_tracking(conn, round_ts)
        log.info(f"[派生] keyword_snapshots +{k}")
        summary["derive_keyword_tracking"] = k
    except Exception as e:
        log.warning(f"[派生] keyword tracking 失败: {e}")

    # --- 主持人智库沉淀：把命中赛道信号词的热榜话题，按电台主持能力匹配后
    #     写成带「电台视角改编角度」的智库条目；跨轮/跨平台复现自动晋升常青 ---
    try:
        import knowledge
        k = knowledge.incubate(conn, round_ts)
        log.info(f"[智库] knowledge_base 沉淀 +{k}")
        summary["derive_knowledge_base"] = k
    except Exception as e:
        log.warning(f"[智库] 沉淀失败（已隔离）: {e}")
    conn.commit()

    summary["totals"] = {
        t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in TABLES
    }
    conn.close()
    ran = [s for s in summary["sources"] if s["status"] != "scheduled_skip"]
    ok = sum(1 for s in ran if s["status"] == "success")
    skipped = len(summary["sources"]) - len(ran)
    log.info(f"===== 轮次完成 {round_ts} | 实跑 {len(ran)} 源（成功 {ok}）"
             f" / 分级跳过 {skipped} | 库内合计 {summary['totals']} =====")
    summary["ran_sources"] = len(ran)
    summary["ok_sources"] = ok
    summary["skipped_sources"] = skipped
    return summary


def main():
    ap = argparse.ArgumentParser(description="创作者情报工作站 采集管线")
    ap.add_argument("--once", action="store_true", help="只跑一轮")
    ap.add_argument("--rounds", type=int, default=1, help="连续采集轮数")
    ap.add_argument("--interval", type=int, default=0,
                    help="轮间隔秒数（0=用配置值）")
    args = ap.parse_args()
    cfg = load_settings()
    gap = args.interval if args.interval > 0 else cfg.get("interval_seconds", 300)
    rounds = 1 if args.once else args.rounds
    for i in range(rounds):
        s = run_once(cfg)
        print(jdump({"round": i + 1, "at": s["round_at"],
                     "sources": s["sources"], "totals": s["totals"]}))
        if i < rounds - 1:
            log.info(f"等待 {gap}s 后进入下一轮 …")
            time.sleep(gap)


if __name__ == "__main__":
    main()
