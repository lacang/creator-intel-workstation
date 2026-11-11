#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""扩展数据源适配器（第二轮优化新增 8 源）。

合规边界与 collector.py 完全一致：仅请求**免登录、公开可访问**的官方榜单 /
热点接口；不带 cookie / 登录态；不生成或伪造 a_bogus / X-Bogus / WBI 等签名；
不触发或绕过验证码、二次验证、设备校验；不采集含实名 / 手机号等 PII 的数据。
不可用的渠道（知乎热榜 API 401、快手/小红书需签名、腾讯热榜空响应）一律不接入。

接入前均已在 2026-11-11 用真实请求逐字段核对结构，详见
01_数据源/数据源测试报告.md（第二轮新增源实测）。

注册表语义（由 collector.py 末尾合并）：
  ORDER       —— 执行顺序（快线在前，慢线/日更在后）
  ADAPTERS    —— source -> 函数(http, cfg, seed) -> 统一结果字典
  LABELS      —— source -> 中文名
  RAW_DIRS    —— source -> 原始响应落盘目录（相对 ROOT）
  SOURCE_META —— source -> {platform, tier, interval, provides, compliance}
"""
from __future__ import annotations

import html
import json
import re
from urllib.parse import quote

ORDER: list[str] = [
    # 快线：轻量、更新快、直接决定「现在该追什么」
    "douyin_hot", "douyin_ies", "weibo_hot", "baidu_hot",
    "toutiao_hot", "netease_hot", "zhihu_search", "bili_square",
    # 慢线：作品维度，含指标快照，风控较严所以降频
    "bili_rank", "bili_popular", "bili_search", "bili_rank_vertical",
    # 日更：静态/垂直榜，变化慢，没必要每轮抓
    "baidu_vertical", "bili_precious",
    # 带货选品源（日更，支撑账号规划「卖什么」数据驱动）
    "smzdm_pick",
]

LABELS: dict[str, str] = {
    "douyin_hot": "抖音官方热点榜",
    "douyin_ies": "抖音热点榜(官方镜像校验)",
    "weibo_hot": "微博热搜",
    "baidu_hot": "百度实时热搜",
    "toutiao_hot": "今日头条热榜",
    "netease_hot": "网易热点新闻",
    "zhihu_search": "知乎热搜词",
    "bili_square": "B站热搜词",
    "bili_rank": "B站全站排行榜",
    "bili_popular": "B站热门",
    "bili_search": "B站关键词搜索",
    "bili_rank_vertical": "B站分区排行榜",
    "baidu_vertical": "百度垂直分榜",
    "baidu_finance": "百度财经榜",
    "baidu_car": "百度汽车榜",
    "baidu_travel": "百度旅游榜",
    "baidu_technology": "百度科技榜",
    "smzdm_pick": "什么值得买·快消选品",
    "bili_precious": "B站入站必刷",
}

RAW_DIRS: dict[str, str] = {
    "douyin_hot": "05_趋势研究/raw",
    "douyin_ies": "05_趋势研究/raw",
    "weibo_hot": "05_趋势研究/raw",
    "baidu_hot": "05_趋势研究/raw",
    "toutiao_hot": "05_趋势研究/raw",
    "netease_hot": "05_趋势研究/raw",
    "zhihu_search": "05_趋势研究/raw",
    "bili_square": "05_趋势研究/raw",
    "bili_rank": "03_作品数据库/raw",
    "bili_popular": "03_作品数据库/raw",
    "bili_search": "03_作品数据库/raw",
    "bili_rank_vertical": "03_作品数据库/raw",
    "baidu_vertical": "05_趋势研究/raw",
    "bili_precious": "03_作品数据库/raw",
    "smzdm_pick": "06_选品数据库/raw",
}

# 数据源元数据：看板「数据源覆盖」直接展示，也是合规审计的依据
SOURCE_META: dict[str, dict] = {
    "douyin_hot":        {"platform": "douyin",   "tier": "快线", "interval": 120,  "provides": "热点词/热度/榜位"},
    "douyin_ies":        {"platform": "douyin",   "tier": "快线", "interval": 120,  "provides": "同榜镜像/交叉校验"},
    "weibo_hot":         {"platform": "weibo",    "tier": "快线", "interval": 120,  "provides": "热搜词/热度/标签"},
    "baidu_hot":         {"platform": "baidu",    "tier": "快线", "interval": 120,  "provides": "实时热搜/热度"},
    "toutiao_hot":       {"platform": "toutiao",  "tier": "快线", "interval": 120,  "provides": "热榜/热度/话题页链接"},
    "netease_hot":       {"platform": "netease",  "tier": "快线", "interval": 120,  "provides": "新闻线索/原文链接/发布时间"},
    "zhihu_search":      {"platform": "zhihu",    "tier": "快线", "interval": 120,  "provides": "热搜词（平台不公开热度）"},
    "bili_square":       {"platform": "bilibili", "tier": "快线", "interval": 120,  "provides": "热搜词/热度分"},
    "bili_rank":         {"platform": "bilibili", "tier": "慢线", "interval": 300,  "provides": "作品/创作者/全量指标"},
    "bili_popular":      {"platform": "bilibili", "tier": "慢线", "interval": 300,  "provides": "作品/创作者/全量指标"},
    "bili_search":       {"platform": "bilibili", "tier": "慢线", "interval": 600,  "provides": "关键词命中作品（风控较严）"},
    "bili_rank_vertical":{"platform": "bilibili", "tier": "慢线", "interval": 300,  "provides": "分区榜作品/创作者"},
    "baidu_vertical":    {"platform": "baidu",    "tier": "日更", "interval": 3600, "provides": "小说/影视/剧集/游戏/财经/汽车/旅游/科技分榜"},
    "baidu_finance":     {"platform": "baidu",    "tier": "日更", "interval": 3600, "provides": "财经口播选题（基金/楼市/消费/理财热点）"},
    "baidu_car":         {"platform": "baidu",    "tier": "日更", "interval": 3600, "provides": "汽车口播选题（车型/降价/新能源/出行）"},
    "baidu_travel":      {"platform": "baidu",    "tier": "日更", "interval": 3600, "provides": "旅行口播选题（目的地/攻略/节庆/避坑）"},
    "baidu_technology":  {"platform": "baidu",    "tier": "日更", "interval": 3600, "provides": "科技口播选题（AI/数码/互联网/硬科技）"},
    "bili_precious":     {"platform": "bilibili", "tier": "日更", "interval": 3600, "provides": "经典作品语料/创作者"},
    "smzdm_pick":        {"platform": "smzdm", "tier": "日更", "interval": 3600, "provides": "快消选品(纸品/预包装食品)商品名/品类/链接", "compliance": "免登录公开页；无登录态、无签名、无PII"},
}

DY_LABELS = {0: "", 1: "新", 2: "荐", 3: "热", 4: "爆", 5: "首", 6: "沸"}


def _int(x):
    try:
        return int(x)
    except Exception:
        return None


def _trend(platform, trend_source, rank, keyword, heat, tag, extra, raw):
    """统一 trend 记录。heat=None 表示平台不公开热度（不是 0，避免污染统计）。"""
    return {
        "kind": "trend", "platform": platform, "trend_source": trend_source,
        "rank": rank, "keyword": keyword, "heat_value": heat, "tag": tag,
        "extra": extra, "raw": raw,
    }


# ---------------------------------------------------------------- 快线 · 热榜

def a_douyin_ies(http, cfg, seed):
    """抖音官方旧域名热点词榜。与主榜 douyin_hot 同源不同接口，用于交叉校验：
    两条独立链路若给出一致的榜首与热度，才能确认「抖音榜没有单点故障/被劫持」。"""
    url = "https://www.iesdouyin.com/web/api/v2/hotsearch/billboard/word/"
    j = http.get(url, "https://www.iesdouyin.com/")
    wl = (j or {}).get("word_list") or [] if isinstance(j, dict) else []
    items = [_trend("douyin", "douyin_ies", i + 1, w.get("word") or "",
                    _int(w.get("hot_value")), DY_LABELS.get(w.get("label"), ""),
                    {"label_raw": w.get("label"), "active_time": (j or {}).get("active_time")},
                    w)
             for i, w in enumerate(wl) if w.get("word")]
    return {"source": "douyin_ies", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条热点词（抖音官方镜像源）" if items else "响应为空",
            "items": items}


def a_toutiao_hot(http, cfg, seed):
    """今日头条热榜 50 条，含话题页原文链接 —— 创作者可直接点开核事实。"""
    url = "https://www.toutiao.com/hot-event/hot-board/?origin=toutiao_pc"
    j = http.get(url, "https://www.toutiao.com/")
    data = (j or {}).get("data") or [] if isinstance(j, dict) else []
    lim = int(cfg.get("limit", 50))
    items = []
    for i, w in enumerate(data[:lim]):
        title = w.get("Title") or w.get("QueryWord") or ""
        if not title:
            continue
        items.append(_trend(
            "toutiao", "toutiao_board", i + 1, title,
            _int(w.get("HotValue")), w.get("Label") or "",
            {"url": w.get("Url"), "cluster_id": str(w.get("ClusterIdStr") or w.get("ClusterId") or ""),
             "image": (w.get("Image") or {}).get("url"),
             "categories": w.get("InterestCategory") or [],
             "query_word": w.get("QueryWord")},
            w))
    return {"source": "toutiao_hot", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条热榜（含原文链接）" if items else "响应为空",
            "items": items}


def a_netease_hot(http, cfg, seed):
    """网易热点新闻：真实新闻标题 + 原文链接 + 发布时间 + 来源媒体。
    对故事向创作者最有用的一类线索（有出处、可核实、带人物与情节）。
    该平台不公开热度，heat_value 留 None，用榜位代替强度。"""
    url = "https://m.163.com/fe/api/hot/news/flow"
    j = http.get(url, "https://news.163.com/")
    lst = ((j or {}).get("data") or {}).get("list") or [] if isinstance(j, dict) else []
    lim = int(cfg.get("limit", 50))
    items = []
    for i, w in enumerate(lst[:lim]):
        title = w.get("title") or ""
        if not title:
            continue
        items.append(_trend(
            "netease", "netease_news", i + 1, title, None, "",
            {"url": w.get("url"), "source_media": w.get("source"),
             "publish_time": w.get("publishTime"), "docid": w.get("docid"),
             "imgsrc": w.get("imgsrc")},
            w))
    return {"source": "netease_hot", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条新闻线索（含原文链接，平台不公开热度）" if items else "响应为空",
            "items": items}


def a_zhihu_search(http, cfg, seed):
    """知乎热搜词（top_search）。热榜 API 需授权返回 401，故只用这个公开入口。"""
    url = "https://www.zhihu.com/api/v4/search/top_search"
    j = http.get(url, "https://www.zhihu.com/")
    words = ((j or {}).get("top_search") or {}).get("words") or [] if isinstance(j, dict) else []
    items = []
    for i, w in enumerate(words):
        q = w.get("query") or w.get("display_query") or ""
        if not q:
            continue
        items.append(_trend("zhihu", "zhihu_top_search", i + 1, q, None, "",
                            {"uuid": w.get("uuid"), "display_query": w.get("display_query")}, w))
    return {"source": "zhihu_search", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条知乎热搜词（平台不公开热度）" if items else "响应为空",
            "items": items}


def a_bili_square(http, cfg, seed):
    """B站搜索框热搜词（search/square），带官方 heat_score，且几乎不触发风控。"""
    url = "https://api.bilibili.com/x/web-interface/search/square?limit=10"
    j = http.get_code0(url, "https://www.bilibili.com/")
    lst = (((j or {}).get("data") or {}).get("trending") or {}).get("list") or [] \
        if isinstance(j, dict) else []
    items = []
    for i, w in enumerate(lst):
        kw = w.get("show_name") or w.get("keyword") or ""
        if not kw:
            continue
        items.append(_trend("bilibili", "bili_square", i + 1, kw,
                            _int(w.get("heat_score")), "",
                            {"keyword_raw": w.get("keyword"), "icon": w.get("icon")}, w))
    return {"source": "bili_square", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条 B站热搜词" if items else "响应为空/风控",
            "items": items}


# ---------------------------------------------------------------- 慢线 · 分区榜

def a_bili_rank_vertical(http, cfg, seed):
    """B站分区排行榜：覆盖全站榜看不到的垂类作品（游戏/知识/生活/数码/影视）。"""
    from collector import _bili_work  # 延迟导入，避免模块加载期循环依赖
    ref = "https://www.bilibili.com/v/popular/rank/all"
    hdr = {"Origin": "https://www.bilibili.com"}
    rids = cfg.get("rids") or [
        {"rid": 4, "name": "游戏"}, {"rid": 36, "name": "知识"},
        {"rid": 160, "name": "生活"}, {"rid": 188, "name": "数码"},
        {"rid": 23, "name": "影视"},
    ]
    lim = int(cfg.get("per_rid_limit", 20))
    items, used, used_url = [], [], ""
    for spec in rids:
        rid, pname = spec.get("rid"), spec.get("name") or str(spec.get("rid"))
        url = f"https://api.bilibili.com/x/web-interface/ranking/v2?rid={rid}&type=all"
        j = http.get_code0(url, ref, extra=hdr)
        lst = ((j or {}).get("data") or {}).get("list") or [] if isinstance(j, dict) else []
        for v in lst[:lim]:
            it = _bili_work(v, "bili_rank_vertical")
            it["metrics"]["extra"]["partition"] = pname
            it["metrics"]["extra"]["partition_rid"] = rid
            items.append(it)
        used.append(f"{pname}({len(lst[:lim])})")
        used_url = url
    return {"source": "bili_rank_vertical", "url": used_url, "ok": bool(items),
            "note": f"{len(items)} 条分区榜作品 / {'、'.join(used)}" if items else "分区榜均被风控",
            "items": items}


def a_bili_precious(http, cfg, seed):
    """B站「入站必刷」经典作品语料：变化极慢，按日更采集，用于长期选题参考。
    注意这不是实时榜，作品发布时间可能是数年前，看板会标注其语料属性。"""
    from collector import _bili_work
    url = "https://api.bilibili.com/x/web-interface/popular/precious?ps=20&pn=1"
    j = http.get_code0(url, "https://www.bilibili.com/")
    lst = ((j or {}).get("data") or {}).get("list") or [] if isinstance(j, dict) else []
    lim = int(cfg.get("limit", 60))
    items = []
    for v in lst[:lim]:
        it = _bili_work(v, "bili_precious")
        it["metrics"]["extra"]["collection"] = "入站必刷（经典语料，非实时榜）"
        items.append(it)
    return {"source": "bili_precious", "url": url, "ok": bool(items),
            "note": f"{len(items)} 条经典作品语料（日更）" if items else "响应为空/风控",
            "items": items}


# ---------------------------------------------------------------- 日更 · 垂直分榜

def a_baidu_vertical(http, cfg, seed):
    """百度垂直分榜（小说/电影/电视剧/游戏/财经/汽车/旅游/科技）。页面内 <!--s-data:...--> 注入块，
    与主榜同一解析机制；每个 tab 用独立 trend_source 避免 UNIQUE 冲突。
    这些垂直榜是单人自拍口播的优质选题燃料：财经/汽车/旅行/科技都是能做出个人 IP 的赛道。"""
    tabs = cfg.get("tabs") or [
        "novel", "movie", "teleplay", "game",
        "finance", "car", "travel", "technology",
    ]
    tab_names = {
        "novel": "小说", "movie": "电影", "teleplay": "电视剧", "game": "游戏",
        "finance": "财经", "car": "汽车", "travel": "旅游", "technology": "科技",
    }
    lim = int(cfg.get("per_tab_limit", 30))
    items, used, last_url = [], [], ""
    for tab in tabs:
        url = f"https://top.baidu.com/board?tab={quote(tab)}"
        t = http.get(url, "https://www.baidu.com/", want="text")
        last_url = url
        if not isinstance(t, str) or not t:
            continue
        m = re.search(r"<!--s-data:(.*?)-->", t, re.S)
        if not m:
            continue
        try:
            d = json.loads(html.unescape(m.group(1)))
        except Exception:
            continue
        n = 0
        for card in d.get("data", {}).get("cards", []):
            for c in card.get("content", [])[:lim]:
                word = c.get("word") or c.get("query") or ""
                if not word:
                    continue
                idx = c.get("index")
                rank = (idx + 1) if isinstance(idx, int) else n + 1
                n += 1
                items.append(_trend(
                    "baidu", f"baidu_{tab}", rank, word,
                    _int(c.get("hotScore")), c.get("hotChange") or "",
                    {"tab": tab, "tab_name": tab_names.get(tab, tab),
                     "desc": c.get("desc"), "url": c.get("url") or c.get("rawUrl"),
                     "img": c.get("img"), "show": c.get("show")},
                    c))
        used.append(f"{tab_names.get(tab, tab)}({n})")
    return {"source": "baidu_vertical", "url": last_url, "ok": bool(items),
            "note": f"{len(items)} 条垂直榜 / {'、'.join(used)}" if items else "分榜解析为空",
            "items": items}


def a_smzdm(http, cfg, seed):
    """什么值得买·快消选品源（带货向，支撑账号规划「卖什么」数据驱动）。

    仅采集免登录公开页：首页 feed 流里 onclick 的 dataLayer.push JSON 含完整商品结构化数据
    （pagetitle 商品名、1~4 级分类、article_id、jump_link）。按用户「纸品 + 预包装食品」品类过滤，
    不直接进入口播选题 PRIMARY_SOURCES，避免污染智库沉淀。
    合规边界：免登录公开页；不带 cookie / 登录态；不生成或伪造签名；不采集 PII。
    热度：平台首页静态页不公开数值热度，heat_value 置 None（heat_available=0，如实标记）。
    """
    url = "https://www.smzdm.com/"
    t = http.get(url, "https://www.smzdm.com/", want="text")
    if not isinstance(t, str) or not t:
        return {"source": "smzdm_pick", "url": url, "ok": False,
                "note": "页面为空/未取到", "items": []}

    onclicks = re.findall(r'onclick="([^"]*)"', t)

    def bucket(c):
        c = c or ''
        if any(x in c for x in ['纸品', '湿巾', '个护', '清洁', '洗', '纸']):
            return '纸品'
        if any(x in c for x in ['食品', '零食', '饮料', '粮油', '生鲜', '乳', '酒', '水']):
            return '预包装食品'
        return None

    seen, items, used = set(), [], []
    rank = 0
    for oc in onclicks:
        pt = re.search(r"'pagetitle'\s*:\s*'([^']+)'", oc)
        c3 = re.search(r"'3级分类'\s*:\s*'([^']*)'", oc)
        c4 = re.search(r"'4级分类'\s*:\s*'([^']*)'", oc)
        aid = re.search(r"'article_id'\s*:\s*'(\d+)'", oc)
        lk = re.search(r"'jump_link'\s*:\s*'([^']+)'", oc)
        if not (pt and aid and c3):
            continue
        k = aid.group(1)
        if k in seen:
            continue
        seen.add(k)
        b = bucket(c3.group(1))
        if not b:
            continue
        rank += 1
        items.append(_trend(
            "smzdm", "smzdm_pick", rank, pt.group(1), None, b,
            {"url": lk.group(1) if lk else "",
             "cat3": c3.group(1),
             "cat4": c4.group(1) if c4 else ""},
            oc[:400]))
        used.append(b)
    from collections import Counter
    dist = dict(Counter(used))
    return {"source": "smzdm_pick", "url": url, "ok": bool(items),
            "note": (f"{len(items)} 条快消选品 / " + "、".join(f"{k}{v}" for k, v in dist.items()))
                    if items else "本页无纸品/食品选品",
            "items": items}


ADAPTERS = {
    "douyin_ies": a_douyin_ies,
    "smzdm_pick": a_smzdm,
    "toutiao_hot": a_toutiao_hot,
    "netease_hot": a_netease_hot,
    "zhihu_search": a_zhihu_search,
    "bili_square": a_bili_square,
    "bili_rank_vertical": a_bili_rank_vertical,
    "baidu_vertical": a_baidu_vertical,
    "bili_precious": a_bili_precious,
}
