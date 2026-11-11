#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创作者情报工作站 —— 实时可视化看板

两种运行模式（共用同一套页面模板）：
  1) 实时服务：python3 tools/dashboard.py [--port 8765]
     浏览器/预览面板打开 http://localhost:8765 ，页面每 30s 自动拉 /api/all 刷新，
     数据直接来自 workstation.db，随调度器持续更新。
  2) 静态快照：python3 tools/dashboard.py --export
     把当前数据库内容内联导出为 创作者情报看板.html，离线可看、便于交付。

看板所有数字均为数据库真实记录，无演示/假数据。
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from flask import Flask, Response, jsonify  # noqa: E402
from collector import (ROOT, DB_PATH, TABLES, SOURCE_LABELS, ORDER,
                       SOURCE_META, FRESHNESS_SECONDS,
                       PRIMARY_SOURCE, PRIMARY_SOURCES,
                       jdump, now_dt)  # noqa: E402
import textmatch  # noqa: E402
import knowledge  # noqa: E402

app = Flask(__name__)
# 模块加载瞬间的代码指纹：watchdog 用它判断服务是否仍跑旧版（不能用请求时的文件 mtime）
CODE_MTIME_AT_START = Path(__file__).resolve().stat().st_mtime

PLAT_COLORS = {
    "douyin": "#161823", "weibo": "#f5a623", "baidu": "#3385ff",
    "bilibili": "#fb7299", "toutiao": "#e8503a",
    "zhihu": "#0a68e0", "netease": "#c8302e",
}
PLAT_NAMES = {
    "douyin": "抖音", "weibo": "微博", "baidu": "百度", "bilibili": "B站",
    "toutiao": "头条", "zhihu": "知乎", "netease": "网易",
}


def _interval_seconds() -> int:
    try:
        return int(json.loads((ROOT / "config" / "settings.json")
                              .read_text(encoding="utf-8")).get("interval_seconds", 120))
    except Exception:
        return 120


def _profile() -> dict:
    """主持人画像：优先读 host_profile.json（电台主持智库的权威配置），
    缺失时退回 creator_profile.json。"""
    for p in (ROOT / "config" / "host_profile.json",
              ROOT / "config" / "creator_profile.json"):
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except Exception:
                continue
    return {}


def ro() -> sqlite3.Connection:
    c = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=15)
    c.row_factory = sqlite3.Row
    return c


def rows(sql, args=()):
    c = ro()
    try:
        return [dict(r) for r in c.execute(sql, args).fetchall()]
    finally:
        c.close()


def scalar(sql, args=()):
    c = ro()
    try:
        r = c.execute(sql, args).fetchone()
        return r[0] if r else None
    finally:
        c.close()


def data_overview():
    c = ro()
    try:
        counts = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in TABLES}
        last_run = c.execute("SELECT MAX(run_at) FROM collection_runs").fetchone()[0]
        rounds = c.execute("SELECT COUNT(DISTINCT run_at) FROM collection_runs").fetchone()[0]
        snap_rounds = c.execute(
            "SELECT COUNT(DISTINCT snapshot_at) FROM work_snapshots").fetchone()[0]
        health = [dict(r) for r in c.execute(
            "SELECT source, target, items_collected, items_seen, status, note "
            "FROM collection_runs WHERE run_at = ? ORDER BY source",
            (last_run,)).fetchall()] if last_run else []
        plat = [dict(r) for r in c.execute(
            "SELECT platform, COUNT(*) n FROM trends GROUP BY platform "
            "ORDER BY n DESC").fetchall()]
        today = now_dt().strftime("%Y-%m-%d")
        today_trends = c.execute(
            "SELECT COUNT(*) FROM trends WHERE trend_time LIKE ?", (today + "%",)).fetchone()[0]
        last_work_snapshot = c.execute(
            "SELECT MAX(snapshot_at) FROM work_snapshots").fetchone()[0]
        sources_registered = c.execute(
            "SELECT COUNT(*) FROM source_catalog WHERE enabled=1").fetchone()[0]
    finally:
        c.close()

    # 实时新鲜度：末轮距今多少秒、距下一次采集还有多少秒。
    # 前端据此做倒计时，用户一眼能看出「这个看板是不是活的」。
    age = None
    if last_run:
        try:
            age = (now_dt() - datetime.fromisoformat(last_run)).total_seconds()
        except Exception:
            age = None
    interval = _interval_seconds()
    next_in = max(0, interval - (age % interval)) if age is not None else None
    return {
        "counts": counts, "last_run": last_run, "rounds": rounds,
        "snapshot_rounds": snap_rounds, "health": health,
        "platform_distribution": plat,
        "generated_at": now_dt().isoformat(timespec="seconds"),
        "source_order": ORDER, "source_labels": SOURCE_LABELS,
        "data_age_seconds": round(age, 1) if age is not None else None,
        "next_update_seconds": round(next_in, 1) if next_in is not None else None,
        "interval_seconds": interval,
        "freshness_threshold_seconds": FRESHNESS_SECONDS,
        "today_trends": today_trends,
        "last_work_snapshot": last_work_snapshot,
        "sources_registered": sources_registered,
        "platform_count": len(plat),
    }


def data_trends(limit=15):
    """同轮各平台主榜对比。按 PRIMARY_SOURCE 过滤，避免主榜/镜像/垂直榜混列。"""
    last = scalar("SELECT MAX(trend_time) FROM trends")
    if not last:
        return {}
    ph = ",".join("?" * len(PRIMARY_SOURCES))
    rs = rows(f"""SELECT platform, trend_source, rank, keyword, heat_value,
                         heat_available, tag
                  FROM trends WHERE trend_time = ? AND trend_source IN ({ph})
                  ORDER BY platform, rank""", (last, *PRIMARY_SOURCES))
    out = {}
    for r in rs:
        out.setdefault(r["platform"], []).append(r)
    return {k: v[:limit] for k, v in out.items()}


def data_works(limit=15):
    last = scalar("SELECT MAX(snapshot_at) FROM work_snapshots")
    if not last:
        return []
    rs = rows("""SELECT w.title, w.url, w.work_id, w.duration, w.publish_time,
                        c.display_name AS creator,
                        s.play_count, s.like_count, s.comment_count,
                        s.collect_count, s.raw_metrics_json
                 FROM work_snapshots s
                 JOIN works w ON w.id = s.work_id
                 LEFT JOIN creators c ON c.id = w.creator_id
                 WHERE s.snapshot_at = ?""", (last,))
    for r in rs:
        try:
            ex = json.loads(r.get("raw_metrics_json") or "{}")
        except Exception:
            ex = {}
        r["category"] = ex.get("category")
        r["score"] = ex.get("score")
        r["coin"] = ex.get("coin")
    rs.sort(key=lambda x: (x.get("play_count") or 0), reverse=True)
    return rs[:limit]


def data_creators(limit=10):
    last = scalar("SELECT MAX(snapshot_at) FROM creator_snapshots")
    if not last:
        return []
    rs = rows("""SELECT c.display_name, c.platform_uid,
                        cs.work_count, cs.total_likes, cs.avg_views,
                        cs.raw_metrics_json
                 FROM creator_snapshots cs
                 JOIN creators c ON c.id = cs.creator_id
                 WHERE cs.snapshot_at = ?""", (last,))
    for r in rs:
        try:
            ex = json.loads(r.get("raw_metrics_json") or "{}")
        except Exception:
            ex = {}
        r["total_views"] = ex.get("total_views")
        r["total_comments"] = ex.get("total_comments")
    rs.sort(key=lambda x: (x.get("total_views") or 0), reverse=True)
    return rs[:limit]


def data_growth(limit=10):
    # 原实现对 work_snapshots 全表扫描，数据量随 2 分钟粒度线性增长会拖慢
    # 每 15s 的刷新。限定最近 12 个作品快照轮次（配合 idx_ws_snap_at 索引）。
    recent = rows("""SELECT DISTINCT snapshot_at FROM work_snapshots
                     ORDER BY snapshot_at DESC LIMIT 12""")
    if not recent:
        return []
    ts = [r["snapshot_at"] for r in recent]
    ph = ",".join("?" * len(ts))
    rs = rows(f"""SELECT s.work_id, s.snapshot_at, s.play_count, s.like_count,
                         w.title, w.url, c.display_name AS creator
                  FROM work_snapshots s
                  JOIN works w ON w.id = s.work_id
                  LEFT JOIN creators c ON c.id = w.creator_id
                  WHERE s.snapshot_at IN ({ph})
                  ORDER BY s.work_id, s.snapshot_at""", tuple(ts))
    groups = {}
    for r in rs:
        groups.setdefault(r["work_id"], []).append(r)
    out = []
    for seq in groups.values():
        if len(seq) < 2:
            continue
        a, b = seq[0], seq[-1]
        out.append({
            "title": b["title"], "url": b["url"], "creator": b["creator"],
            "rounds": len(seq),
            "view_from": a["play_count"], "view_to": b["play_count"],
            "delta_view": (b["play_count"] or 0) - (a["play_count"] or 0),
            "delta_like": (b["like_count"] or 0) - (a["like_count"] or 0),
        })
    out.sort(key=lambda x: x["delta_view"], reverse=True)
    return out[:limit]


def data_topics(limit=20):
    return rows("""SELECT title, priority, supply_analysis, differentiation,
                          created_at
                   FROM topic_opportunities
                   ORDER BY CASE priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 ELSE 2 END,
                            id DESC LIMIT ?""", (limit,))


def data_runs(limit=24):
    return rows("""SELECT run_at, source, target, items_collected, status, note
                   FROM collection_runs ORDER BY id DESC LIMIT ?""", (limit,))


def data_timeline():
    rs = rows("""SELECT run_at, source, items_collected, status
                 FROM collection_runs ORDER BY run_at, source""")
    out = {}
    for r in rs:
        g = out.setdefault(r["run_at"], {"run_at": r["run_at"], "total": 0, "by_source": {}})
        g["by_source"][r["source"]] = r["items_collected"]
        g["total"] += r["items_collected"] or 0
    return list(out.values())


def _norm_trend(s):
    return re.sub(r"[\s\W_]+", "", (s or "")).lower()


def data_movements():
    """对比最近两轮热榜：新进榜 / 上升最快 / 退出榜 / 持续在榜。"""
    rs = rows("SELECT DISTINCT trend_time FROM trends ORDER BY trend_time DESC LIMIT 2")
    if len(rs) < 2:
        return {"has_data": False, "note": "需至少 2 轮热榜数据"}
    cur_t, prev_t = rs[0]["trend_time"], rs[1]["trend_time"]

    def snap(t):
        d = {}
        for r in rows("SELECT platform, keyword, rank, heat_value, tag "
                      "FROM trends WHERE trend_time = ?", (t,)):
            d[(r["platform"], _norm_trend(r["keyword"]))] = r
        return d

    a, b = snap(prev_t), snap(cur_t)
    new = [v for k, v in b.items() if k not in a]
    gone = [v for k, v in a.items() if k not in b]
    rising = []
    for k, v in b.items():
        if k in a and v["rank"] and a[k]["rank"] and v["rank"] < a[k]["rank"]:
            rising.append({**v, "from_rank": a[k]["rank"],
                           "delta": a[k]["rank"] - v["rank"]})
    new.sort(key=lambda x: (x["rank"] or 999))
    rising.sort(key=lambda x: -x["delta"])
    gone.sort(key=lambda x: (x["rank"] or 999))
    return {
        "has_data": True, "prev_round": prev_t, "cur_round": cur_t,
        "new": new[:10], "rising": rising[:10], "gone": gone[:10],
        "stayed": len(set(a) & set(b)),
        "new_count": len(new), "rising_count": len(rising), "gone_count": len(gone),
    }


def data_keywords():
    """关注词追踪：最近一轮命中 + 每词最近若干轮的命中平台数走势。"""
    last = scalar("SELECT MAX(round_at) FROM keyword_snapshots")
    if not last:
        return {"has_data": False, "keywords": [], "rounds": []}
    rs = rows("""SELECT k.keyword, k.category, s.hits, s.platforms,
                        s.best_rank, s.total_heat, s.matched_words
                 FROM keyword_snapshots s
                 JOIN tracked_keywords k ON k.id = s.keyword_id
                 WHERE s.round_at = ?
                 ORDER BY s.hits DESC, s.total_heat DESC""", (last,))
    rounds = [r["round_at"] for r in rows(
        "SELECT DISTINCT round_at FROM keyword_snapshots ORDER BY round_at DESC LIMIT 8")]
    rounds = list(reversed(rounds))
    series = {}
    if rounds:
        ph = ",".join("?" * len(rounds))
        for r in rows(f"""SELECT k.keyword, s.round_at, s.hits, s.total_heat
                          FROM keyword_snapshots s
                          JOIN tracked_keywords k ON k.id = s.keyword_id
                          WHERE s.round_at IN ({ph})""", tuple(rounds)):
            series.setdefault(r["keyword"], {})[r["round_at"]] = {
                "hits": r["hits"], "heat": r["total_heat"]}
    out = []
    for r in rs:
        s = series.get(r["keyword"], {})
        out.append({**r,
                    "trend": [{"round": t, "hits": s.get(t, {}).get("hits", 0),
                               "heat": s.get(t, {}).get("heat", 0)} for t in rounds],
                    "hit_rounds": sum(1 for t in rounds if s.get(t, {}).get("hits", 0) > 0)})
    return {"has_data": True, "keywords": out, "rounds": rounds, "at": last}


def data_source_stats():
    return rows("""SELECT source,
                          COUNT(*) AS runs,
                          SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS ok,
                          SUM(items_collected) AS items,
                          MAX(run_at) AS last
                   FROM collection_runs
                   WHERE status <> 'scheduled_skip'
                   GROUP BY source ORDER BY source""")


def data_news(limit=18):
    """可点击原文的新闻线索（网易热点 + 头条热榜）。
    热榜词只告诉你「什么在热」，新闻源还给出标题、原文链接、发布时间与媒体，
    故事向创作者可以直接点开核事实 —— 这是本轮新增源最实用的一层。"""
    last = scalar("SELECT MAX(trend_time) FROM trends "
                  "WHERE trend_source IN ('netease_news','toutiao_board')")
    if not last:
        return {"has_data": False, "items": [], "at": None}
    rs = rows("""SELECT platform, trend_source, rank, keyword, heat_value,
                        heat_available, _raw_json
                 FROM trends WHERE trend_time = ?
                   AND trend_source IN ('netease_news','toutiao_board')
                 ORDER BY rank LIMIT ?""", (last, limit))
    cats = _profile().get("narrative_categories", [])
    items = []
    for r in rs:
        try:
            extra = json.loads(r.get("_raw_json") or "{}").get("extra") or {}
        except Exception:
            extra = {}
        hit_cats = [c["name"] for c in cats
                    if any(s in (r["keyword"] or "") for s in c.get("signals", []))]
        items.append({
            "platform": r["platform"], "source": r["trend_source"],
            "rank": r["rank"], "keyword": r["keyword"],
            "heat": r["heat_value"] if r["heat_available"] else None,
            "url": extra.get("url"), "media": extra.get("source_media"),
            "publish_time": extra.get("publish_time"),
            "categories": hit_cats[:2],
        })
    return {"has_data": True, "at": last, "items": items}


def data_source_coverage():
    """数据源覆盖：从 source_catalog 直接读元数据 + 末轮各源执行状态。
    让「14 源 / 7 平台 / 各自多久采一次」变成页面上可核查的事实。"""
    try:
        cat = rows("""SELECT name, label, platform, tier, interval_s, provides,
                             enabled, compliance
                      FROM source_catalog
                      ORDER BY CASE tier WHEN '快线' THEN 0 WHEN '慢线' THEN 1 ELSE 2 END,
                               name""")
    except Exception:
        cat = []
    last = scalar("SELECT MAX(run_at) FROM collection_runs")
    st = {}
    if last:
        for r in rows("""SELECT source, status, items_collected, items_seen, note
                         FROM collection_runs WHERE run_at = ?""", (last,)):
            st[r["source"]] = r
    for x in cat:
        x["run"] = st.get(x["name"])
    plats = sorted({x["platform"] for x in cat})
    return {
        "sources": cat, "platforms": plats,
        "platform_count": len(plats), "source_count": len(cat),
        "last_round": last,
        "ran": sum(1 for x in cat if x["run"] and
                   x["run"]["status"] not in ("scheduled_skip",)),
        "skipped": sum(1 for x in cat if x["run"] and
                       x["run"]["status"] == "scheduled_skip"),
    }


def data_crosscheck():
    """抖音双源交叉校验：主榜与官方镜像榜的 TOP10 重合度。
    两条独立链路一致，才能说明「抖音热点不是单一接口的缓存/劫持」。"""
    last = scalar("SELECT MAX(trend_time) FROM trends")
    if not last:
        return {"has_data": False}
    a = rows("""SELECT keyword, rank FROM trends WHERE trend_time = ?
                AND trend_source = 'douyin_hot' ORDER BY rank LIMIT 10""", (last,))
    b = rows("""SELECT keyword, rank FROM trends WHERE trend_time = ?
                AND trend_source = 'douyin_ies' ORDER BY rank LIMIT 10""", (last,))
    sa = {textmatch.norm(x["keyword"]) for x in a}
    sb = {textmatch.norm(x["keyword"]) for x in b}
    union = sa | sb
    return {
        "has_data": bool(a and b), "at": last,
        "head_a": a[0]["keyword"] if a else None,
        "head_b": b[0]["keyword"] if b else None,
        "head_match": bool(a and b and textmatch.norm(a[0]["keyword"])
                           == textmatch.norm(b[0]["keyword"])),
        "overlap": len(sa & sb), "top_n": 10,
        "jaccard": round(len(sa & sb) / len(union), 3) if union else 0,
    }


def data_brief():
    """今日情报简报：把全网热榜收敛成『创作者今天该看什么、能拍什么』。

    首屏专用。所有数字取自末轮真实 trends；改编角度来自可配置的赛道画像
    config/creator_profile.json（规则派生、非生成式预测，明确标注需人工筛选）。

    第二轮改动：共振改用 textmatch 的「精确 + 受控模糊」聚类，让措辞不同的
    同一事件（尤其抖音 vs 微博/百度）能关联起来；每条共振带 match/evidence，
    模糊结果在看板上显式标注为推断，不伪装成平台确认的事实。
    """
    prof = _profile()
    cats = prof.get("narrative_categories", [])
    last = scalar("SELECT MAX(trend_time) FROM trends")
    if not last:
        return {"has_data": False, "note": "暂无热榜数据"}
    ph = ",".join("?" * len(PRIMARY_SOURCES))
    cur = rows(f"""SELECT id, platform, trend_source, keyword, rank,
                          heat_value, heat_available, tag
                   FROM trends WHERE trend_time = ? AND trend_source IN ({ph})""",
               (last, *PRIMARY_SOURCES))

    # 1) 跨平台共振（精确相等 + 受控模糊共现）
    clusters = textmatch.resonance(cur, min_platforms=2, max_clusters=40)
    resonance = [{
        "keyword": g["keyword"], "platforms": g["platforms"],
        "best_rank": g["best_rank"], "max_heat": g["max_heat"],
        "match": g["match"], "evidence": g["evidence"],
        "platform_count": g["platform_count"],
    } for g in clusters]

    # 2) 上升 / 新进：与上一轮同口径主榜对比，找切入窗口
    prev = scalar(
        f"SELECT MAX(trend_time) FROM trends WHERE trend_time < ? "
        f"AND trend_source IN ({ph})", (last, *PRIMARY_SOURCES))
    rising, fresh = [], []
    if prev:
        prev_rows = rows(f"""SELECT platform, keyword, rank, heat_value FROM trends
                            WHERE trend_time = ? AND trend_source IN ({ph})""",
                         (prev, *PRIMARY_SOURCES))

        def snap(rs):
            return {(r["platform"], textmatch.norm(r["keyword"])): r for r in rs}

        a, b = snap(prev_rows), snap(cur)
        for k, v in b.items():
            if k in a and v["rank"] and a[k]["rank"] and v["rank"] < a[k]["rank"]:
                rising.append({"keyword": v["keyword"], "platform": v["platform"],
                               "rank": v["rank"], "delta": a[k]["rank"] - v["rank"]})
            elif k not in a:
                fresh.append({"keyword": v["keyword"], "platform": v["platform"],
                              "rank": v["rank"]})
        rising.sort(key=lambda x: -x["delta"])
        fresh.sort(key=lambda x: (x["rank"] or 999))

    # 3) 故事向可改编：命中赛道信号词，附改编角度；共振/上升的排前面
    res_keys = {textmatch.norm(r["keyword"]) for r in resonance}
    rise_keys = {textmatch.norm(r["keyword"]) for r in rising[:10]}
    picks, seen = [], set()
    for r in sorted(cur, key=lambda x: (x["platform"] != "douyin", x["rank"] or 999)):
        nk = textmatch.norm(r["keyword"])
        if nk in seen:
            continue
        for cat in cats:
            hit = [s for s in cat["signals"] if s in (r["keyword"] or "")]
            if not hit:
                continue
            seen.add(nk)
            picks.append({
                "keyword": r["keyword"], "platform": r["platform"],
                "rank": r["rank"], "heat": r["heat_value"],
                "category": cat["name"], "angle": cat["angle"],
                "signals": hit[:3],
                "resonance": nk in res_keys, "rising": nk in rise_keys,
            })
            break
    picks.sort(key=lambda x: (not x["resonance"], not x["rising"], x["rank"] or 999))

    shown = resonance[:8]
    # 「今天优先拍 3 个」按叙事角度去重：同一信号类别只取一条，
    # 避免三条选题给出完全相同的改编建议（可读性与实用性都更好）。
    top, tcats = [], set()
    for p in picks:
        if p["category"] in tcats:
            continue
        tcats.add(p["category"])
        top.append(p)
        if len(top) >= 3:
            break
    return {
        "has_data": True, "at": last,
        "account": prof.get("account_hint") or prof.get("identity", ""),
        "style": prof.get("content_style") or prof.get("one_liner", ""),
        "douyin_top": [{"keyword": r["keyword"], "rank": r["rank"], "heat": r["heat_value"]}
                       for r in sorted([x for x in cur if x["platform"] == "douyin"],
                                       key=lambda x: (x["rank"] or 999))[:8]],
        "resonance": shown,
        "rising": rising[:6], "fresh": fresh[:6],
        "picks": picks[:12], "top_picks": top,
        # 统计口径与首屏展示一致（只算 shown 的 8 条），另给总数供核对
        "exact_resonance": sum(1 for r in shown if r["match"] == "exact"),
        "fuzzy_resonance": sum(1 for r in shown if r["match"] == "fuzzy"),
        "total_resonance_clusters": len(resonance),
        "disclaimer": ("榜位/热度为末轮真实采集；共振含受控模糊匹配（推断，已标注）；"
                       "改编角度为规则派生、需人工二次筛选，非预测或事实补充"),
    }


def data_maintenance():
    """读取每次 cron 维护的执行留痕，看板显示「系统仍在运行」的时间线。"""
    p = ROOT / "logs" / "maintenance_history.json"
    if not p.exists():
        return {"records": []}
    try:
        recs = json.loads(p.read_text(encoding="utf-8"))
        return {"records": list(reversed(recs[-12:]))}
    except Exception:
        return {"records": []}


def data_quality():
    """读取 tools/verify.py 的数据质量自检结果。"""
    p = ROOT / "logs" / "quality.json"
    if not p.exists():
        return {"healthy": None, "score": None, "issues": [], "warnings": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"healthy": None, "score": None, "issues": [], "warnings": []}


def data_continuity():
    """实时计算数据连续性：直接据 DB 末轮时间算滞后，不依赖 cron 写的静态快照。

    这是关键防呆：health.json 是维护任务写的一次性快照，VM 休眠 / cron 停摆时
    它会冻结成「healthy=true、距上次 0.3 分钟」，让看板在服务已挂时仍显示绿。
    改为每次请求实时算，无论调度器死活，看板显示的都是此刻真实状态。
    """
    from datetime import datetime
    c = ro()
    try:
        runs = [r[0] for r in c.execute(
            "SELECT DISTINCT run_at FROM collection_runs ORDER BY run_at")]
    finally:
        c.close()
    if not runs:
        return {"healthy": None, "rounds": 0, "gaps": [],
                "note": "数据库暂无采集记录"}
    ts = [datetime.fromisoformat(x) for x in runs]
    now = datetime.now(ts[-1].tzinfo)
    since = (now - ts[-1]).total_seconds()
    gaps = []
    for i in range(1, len(ts)):
        d = (ts[i] - ts[i - 1]).total_seconds()
        if d > FRESHNESS_SECONDS:
            gaps.append({"from": runs[i - 1], "to": runs[i],
                         "minutes": round(d / 60, 1)})
    max_gap = max([(ts[i] - ts[i - 1]).total_seconds()
                   for i in range(1, len(ts))] or [0])
    interval = _interval_seconds()
    return {
        "rounds": len(runs),
        "first": runs[0],
        "last": runs[-1],
        "since_last_minutes": round(since / 60, 1),
        "since_last_seconds": round(since, 1),
        "next_update_seconds": round(max(0, interval - (since % interval)), 1),
        "interval_seconds": interval,
        "freshness_threshold_seconds": FRESHNESS_SECONDS,
        "max_gap_minutes": round(max_gap / 60, 1),
        "gaps": gaps,
        "historical_gaps": len(gaps),
        "stale": since > FRESHNESS_SECONDS,
        "healthy": since <= FRESHNESS_SECONDS,
        "checked_at": now.isoformat(timespec="seconds"),
        "live": True,
    }


def data_knowledge_base():
    """主持人智库 / 起号准备工作台。把 knowledge_base 沉淀结果聚合为可直接用的准备视图。

    分层：
      * 定位卡 —— 你是谁、你最强的能力、适合什么形式（来自 host_profile.json）
      * 内容框架库 —— 可复用的主持人内容结构（开场声音钩子/访谈式拆解…）
      * 选题库 —— 常青(已验证) / 孵化中 / 待拍，按 durability 排序
      * 准备清单 —— 起号前还差什么（定位/框架/常青选题/待拍）
    所有数字来自 knowledge_base 真实沉淀，不编、不渲染空数据。
    """
    prof = _profile()
    c = ro()
    try:
        kb = [dict(r) for r in c.execute(
            "SELECT * FROM knowledge_base ORDER BY "
            "CASE status WHEN 'ready' THEN 0 WHEN 'validated' THEN 1 ELSE 2 END, "
            "durability DESC, last_seen DESC").fetchall()]
        assets = [dict(r) for r in c.execute(
            "SELECT asset_key, name, maps_to, strength FROM host_assets "
            "WHERE enabled=1 ORDER BY strength DESC").fetchall()]
        fws = [dict(r) for r in c.execute(
            "SELECT fw_key, name, fit_asset, structure, hook_example FROM "
            "content_frameworks WHERE enabled=1 ORDER BY name").fetchall()]
        afw = {f["fw_key"]: f for f in fws}
        counts = {s: c.execute(
            "SELECT COUNT(*) FROM knowledge_base WHERE status=?", (s,)).fetchone()[0]
            for s in ("incubating", "validated", "ready")}
        cats = c.execute(
            "SELECT category, COUNT(*) n FROM knowledge_base GROUP BY category "
            "ORDER BY n DESC").fetchall()
        # 常青选题（validated + ready，按 durability 降序）= 起号后可反复做的内容
        evergreen = [k for k in kb if k["status"] in ("validated", "ready")]
        incubating = [k for k in kb if k["status"] == "incubating"]
        # 内容安全：命中拒识清单的条目不进入面向创作者的选题推荐面（底层数据原样保留）
        excluded_ids = {id(k) for k in kb if knowledge._is_sensitive(k.get("nk") or "")}
        evergreen = [k for k in evergreen if id(k) not in excluded_ids]
        incubating = [k for k in incubating if id(k) not in excluded_ids]
    finally:
        c.close()

    fw_of = lambda key: afw.get(key, {}).get("name", "")
    asset_by_key = {a.get("asset_key"): a for a in assets}
    for k in kb:
        try:
            k["platforms"] = json.loads(k["platforms_seen"] or "[]")
        except Exception:
            k["platforms"] = []
        k["fw_name"] = fw_of(k["framework_key"])
        # 口播脚本骨架：把匹配到的框架模板 + 能力，合成可直接照着拍的 4 步脚本
        fw = afw.get(k["framework_key"], {})
        asset = asset_by_key.get(k["asset_key"], {})
        k["script"] = knowledge._script_skeleton(asset, fw)
        k["asset_name"] = asset.get("name", "")

    # 今日口播选题（quick pick）：常青里 durability 最高的几条，起号前就能反复拍
    picks = [x for x in evergreen if id(x) not in excluded_ids][:6]

    # 起号准备清单（可解释、可勾选）
    checklist = [
        {"item": "定位卡已明确", "done": bool(prof.get("positioning_draft"))},
        {"item": "口播能力库已就位",
         "done": len(assets) >= 4},
        {"item": "可复用口播框架 ≥5 个（含脚本骨架）",
         "done": len([f for f in fws if f.get("skeleton")]) >= 5},
        {"item": "常青选题库已积累（≥5 条已验证）",
         "done": len(evergreen) >= 5},
        {"item": "已有可立即拍的待拍素材（≥1 条 ready）",
         "done": counts.get("ready", 0) >= 1},
    ]
    done_n = sum(1 for x in checklist if x["done"])

    return {
        "has_data": len(kb) > 0,
        "identity": prof.get("identity", ""),
        "one_liner": prof.get("one_liner", ""),
        "positioning_draft": prof.get("positioning_draft", ""),
        "suitable_formats": prof.get("suitable_formats", []),
        "assets": assets,
        "frameworks": fws,
        "counts": counts,
        "category_dist": [dict(r) for r in cats],
        "evergreen": evergreen[:12],
        "incubating": incubating[:12],
        "picks": picks,
        "all": [k for k in kb[:40] if id(k) not in excluded_ids],
        "excluded_sensitive": len(excluded_ids),
        "checklist": checklist,
        "checklist_done": done_n,
        "total": len(kb),
        "disclaimer": ("智库由实时热榜按你的单人自拍口播能力自动沉淀；角度与脚本骨架均为规则派生、需人工二次筛选，"
                       "不是事实补充或流量承诺。常青选题=跨轮/跨平台反复出现、已验证可反复做的内容。"
                       f"已按内容安全策略隔离 {len(excluded_ids)} 条时政/敏感话题，不向个人创作者推荐为选题。"),
    }


def data_smzdm():
    """快消选品情报（带货向，支撑账号规划「卖什么」数据驱动）。

    读 trends 表 smzdm_pick 源（什么值得买首页快消选品，采集时已按「纸品+预包装食品」
    过滤入库）。按品类桶分组，供「选品情报」面板直接展示。热度平台不公开，按采集时间排序。
    """
    c = ro()
    try:
        rows = c.execute(
            "SELECT keyword, tag, _raw_json, trend_time FROM trends "
            "WHERE trend_source='smzdm_pick' ORDER BY trend_time DESC, rank").fetchall()
    finally:
        c.close()
    paper, food, seen, latest = [], [], set(), None
    for r in rows:
        if latest is None:
            latest = r["trend_time"]
        kw = r["keyword"]
        if not kw or kw in seen:
            continue
        seen.add(kw)
        extra = {}
        try:
            extra = json.loads(r["_raw_json"] or "{}").get("extra", {})
        except Exception:
            pass
        item = {"title": kw, "cat4": extra.get("cat4", ""),
                "cat3": extra.get("cat3", ""), "url": extra.get("url", "")}
        if r["tag"] == "纸品":
            paper.append(item)
        elif r["tag"] == "预包装食品":
            food.append(item)
    return {
        "has_data": len(paper) + len(food) > 0,
        "paper": paper[:15], "food": food[:15],
        "total_paper": len(paper), "total_food": len(food),
        "updated_at": latest,
        "disclaimer": ("选品数据来自什么值得买公开页（免登录、无 PII），按你的「纸品+预包装食品」方向过滤。"
                       "商品热度平台不公开，按采集时间排序；具体卖点/价格以商品页为准，需人工二次筛选。"),
    }


def all_data():
    return {
        "brief": data_brief(),
        "overview": data_overview(),
        "trends": data_trends(15),
        "works": data_works(15),
        "creators": data_creators(10),
        "growth": data_growth(10),
        "topics": data_topics(20),
        "runs": data_runs(24),
        "timeline": data_timeline(),
        "continuity": data_continuity(),
        "movements": data_movements(),
        "keywords": data_keywords(),
        "source_stats": data_source_stats(),
        "coverage": data_source_coverage(),
        "crosscheck": data_crosscheck(),
        "news": data_news(),
        "knowledge": data_knowledge_base(),
        "smzdm": data_smzdm(),
        "maintenance": data_maintenance(),
        "quality": data_quality(),
    }


EXPORT_TABLES = {
    "trends", "works", "creators", "work_snapshots", "creator_snapshots",
    "content_analysis", "topic_opportunities", "collection_runs",
    "tracked_keywords", "keyword_snapshots", "source_catalog",
}


@app.route("/api/export/<name>.csv")
def export_csv(name):
    """把指定表整表导出为 CSV（UTF-8 BOM，Excel 可直接打开）。"""
    if name not in EXPORT_TABLES:
        return Response("不允许导出该表", status=404)
    c = ro()
    try:
        cur = c.execute(f"SELECT * FROM {name}")
        cols = [d[0] for d in cur.description]
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(cols)
        for row in cur.fetchall():
            w.writerow(["" if v is None else v for v in row])
    finally:
        c.close()
    return Response("\ufeff" + buf.getvalue(),
                    mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f'attachment; filename="{name}.csv"'})


@app.route("/api/export/all.json")
def export_all_json():
    return Response(jdump(all_data()),
                    mimetype="application/json; charset=utf-8",
                    headers={"Content-Disposition": 'attachment; filename="workstation_all.json"'})


@app.route("/api/version")
def api_version():
    """供 watchdog 判断服务实际加载的代码版本（模块加载时快照，非请求时）。"""
    return jsonify({"code_mtime": CODE_MTIME_AT_START, "pid": os.getpid()})


_heal_lock = threading.Lock()
_last_heal_try = [0.0]


def maybe_self_heal():
    """服务存活但数据停滞时（例如沙箱休眠后系统级 cron 未能恢复），
    用户一访问看板即在后台补采一轮，不让"能打开页面"却"数据是旧的"。

    非阻塞、带节流与并发锁：补采在守护线程里跑，120 秒内最多尝试一次。
    系统级 cron 恢复后本逻辑自动让位（那时数据不 stale，不会重复采）。
    """
    now = time.time()
    if now - _last_heal_try[0] < 120:
        return
    try:
        if data_continuity().get("stale") is not True:
            return
    except Exception:
        return
    if not _heal_lock.acquire(blocking=False):
        return
    _last_heal_try[0] = now

    def _job():
        try:
            import collector
            collector.run_once()
        except Exception:
            pass
        finally:
            _heal_lock.release()

    threading.Thread(target=_job, daemon=True).start()


@app.route("/api/kb/<int:kb_id>/promote", methods=["POST"])
def api_kb_promote(kb_id):
    """把一条智库条目升级为「待拍素材」（用户策展，不覆盖自动沉淀）。"""
    try:
        import knowledge
        ok = knowledge.promote(kb_id)
        return jsonify({"ok": ok, "error": None if ok else "条目不存在"})
    except Exception as e:
        return jsonify({"ok": False, "error": f"{type(e).__name__}: {e}"}), 500


@app.route("/api/all")
def api_all():
    maybe_self_heal()
    return jsonify(all_data())


@app.route("/")
def index():
    return Response(render(None), mimetype="text/html; charset=utf-8")


# ---------------------------------------------------------------- 页面模板
HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>创作者情报工作站 · 实时数据库看板</title>
<style>
  :root{
    --bg:#f4f6fa; --card:#ffffff; --ink:#1b2333; --muted:#7b879c;
    --line:#e6eaf2; --accent:#2f5bea; --ok:#12a594; --warn:#e0842a; --bad:#d94b4b;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--ink);
       font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}
  .wrap{max-width:1440px;margin:0 auto;padding:22px 26px 60px}
  header{display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:18px}
  h1{font-size:22px;margin:0;letter-spacing:.5px}
  h1 small{font-size:12px;color:var(--muted);font-weight:400;margin-left:10px}
  .meta{font-size:12px;color:var(--muted)}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--ok);margin-right:6px}
  button{border:1px solid var(--line);background:#fff;border-radius:7px;padding:6px 12px;
         cursor:pointer;font-size:12px;color:var(--ink)}
  button:hover{border-color:var(--accent);color:var(--accent)}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:18px}
  .card{background:var(--card);border:1px solid var(--line);border-radius:11px;padding:14px 16px;
        box-shadow:0 1px 3px rgba(20,40,80,.04)}
  .card .num{font-size:26px;font-weight:650;letter-spacing:.5px}
  .card .lab{font-size:12px;color:var(--muted);margin-top:2px}
  .grid2{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:1000px){.grid2{grid-template-columns:1fr}}
  .panel{background:var(--card);border:1px solid var(--line);border-radius:11px;
         padding:16px 18px;margin-bottom:16px;box-shadow:0 1px 3px rgba(20,40,80,.04)}
  .panel h2{font-size:15px;margin:0 0 12px;display:flex;align-items:center;gap:8px}
  .panel h2 .sub{font-size:11px;color:var(--muted);font-weight:400}
  table{width:100%;border-collapse:collapse;font-size:12.5px}
  th,td{text-align:left;padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:top}
  th{color:var(--muted);font-weight:500;font-size:11.5px;white-space:nowrap}
  tr:last-child td{border-bottom:none}
  td.r,th.r{text-align:right;white-space:nowrap}
  a{color:var(--accent);text-decoration:none}
  a:hover{text-decoration:underline}
  .tag{display:inline-block;padding:1px 7px;border-radius:20px;font-size:11px;
       background:#eef2fb;color:#42527a;white-space:nowrap}
  .tag.hot{background:#fff0e8;color:#d2601a}
  .tag.new{background:#eaf7f1;color:#0e8f6f}
  .pill{display:inline-block;width:7px;height:7px;border-radius:2px;margin-right:6px}
  .st-success{color:var(--ok)} .st-partial{color:var(--warn)} .st-blocked{color:var(--bad)}
  .bar{height:5px;background:#eef1f7;border-radius:4px;overflow:hidden;margin-top:4px;min-width:60px}
  .bar>i{display:block;height:100%;background:linear-gradient(90deg,#5b7cf0,#2f5bea)}
  .bar.dy>i{background:linear-gradient(90deg,#4a4f63,#161823)}
  .bar.wb>i{background:linear-gradient(90deg,#ffc06a,#f5a623)}
  .bar.bd>i{background:linear-gradient(90deg,#7fb0ff,#3385ff)}
  .prio{font-weight:600} .prio.P0{color:#d94b4b} .prio.P1{color:#e0842a} .prio.P2{color:#7b879c}
  .empty{color:var(--muted);font-size:12.5px;padding:10px 0}
  .heat{font-variant-numeric:tabular-nums;color:#42527a}
  .plat-tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:10px}
  .plat-tabs span{font-size:11.5px;padding:3px 10px;border-radius:20px;background:#f0f3f9;color:#42527a}
  footer{margin-top:22px;padding:16px 18px;background:#eef2f8;border:1px solid var(--line);
         border-radius:11px;font-size:12px;color:#5b6782;line-height:1.8}
  .tl{display:flex;gap:4px;align-items:flex-end;height:52px;margin-top:6px}
  .tl .col{flex:1;background:#dfe7f8;border-radius:3px 3px 0 0;min-height:3px;position:relative}
  .tl .col:hover{background:#9db4ee}
  .scroll{max-height:430px;overflow:auto}
  .panel.brief{border:1px solid #c9dbff;background:linear-gradient(180deg,#f7faff,#fff)}
  .brief .lead{font-size:13px;color:#33415e;margin-bottom:12px;line-height:1.7}
  .brief .lead b{color:#1c2c4c}
  .brief .cols{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:1000px){.brief .cols{grid-template-columns:1fr}}
  .brief h3{font-size:12.5px;margin:0 0 8px;color:#2a3a5c;display:flex;align-items:center;gap:7px}
  .brief h3 .cnt{font-size:11px;color:var(--muted);font-weight:400}
  .brief .row{padding:8px 0;border-bottom:1px dashed var(--line);font-size:12.5px}
  .brief .row:last-child{border-bottom:none}
  .brief .row .kw{font-weight:600;color:#1c2c4c}
  .brief .row .ang{color:#5b6782;font-size:11.5px;line-height:1.6;margin-top:3px}
  .brief .rk{color:#d2601a;font-weight:600;font-variant-numeric:tabular-nums}
  .brief .up{color:#0e8f6f;font-weight:600}
  .brief .pl{font-size:10.5px;color:#42527a;background:#eef2fb;border-radius:20px;padding:1px 7px;margin-left:5px}
  .brief .cat{font-size:10.5px;color:#42527a;background:#f0eefb;border-radius:4px;padding:1px 6px;margin-right:6px}
  .brief .disc{font-size:11px;color:var(--muted);margin-top:12px;border-top:1px solid var(--line);padding-top:9px;line-height:1.6}
  .brief .go{background:#fff6e8;border:1px solid #f0dca8;border-radius:8px;padding:10px 13px;
             font-size:12.5px;color:#7a5a10;margin-bottom:14px;line-height:1.7}
  .brief .go b{color:#a33}
  .ops{margin-bottom:16px;border:1px solid var(--line);border-radius:11px;background:var(--card)}
  .ops>summary{cursor:pointer;padding:13px 18px;font-size:14px;font-weight:600;list-style:none}
  .ops>summary::-webkit-details-marker{display:none}
  .ops>summary::before{content:'▸';margin-right:9px;color:var(--accent)}
  .ops[open]>summary::before{content:'▾'}
  .ops>summary .sub{font-weight:400}
  .ops .panel{margin:0 14px 14px;box-shadow:none}
  .exp{display:inline-block;margin:0 6px 6px 0;padding:4px 10px;border:1px solid var(--line);
       border-radius:6px;font-size:11.5px;background:#f8fafd;color:var(--accent)}
  .exp:hover{border-color:var(--accent);text-decoration:none;background:#eef3ff}
  .mini{display:inline-flex;gap:2px;align-items:flex-end;height:16px;vertical-align:middle}
  .mini i{display:inline-block;width:5px;background:#c9d6f2;border-radius:1px 1px 0 0;min-height:2px}
  .mini i.on{background:#2f5bea}
  .mv-col{width:33.3%;vertical-align:top;padding-right:12px}
  /* 第二轮：实时新鲜度条 */
  .freshbar{display:flex;align-items:center;gap:14px;flex-wrap:wrap;padding:10px 16px;
    margin-bottom:16px;border-radius:10px;background:#eef7f4;border:1px solid #cbe9df;font-size:12.5px}
  .freshbar.warn{background:#fff8e8;border-color:#f0dca8}
  .freshbar.bad{background:#fff2f2;border-color:#f3c9c9}
  .freshbar b{font-size:13px}
  .freshbar .cd{font-variant-numeric:tabular-nums;font-weight:600}
  .freshbar .sep{color:var(--muted)}
  /* 第二轮：匹配性质 / 新闻 / 数据源覆盖 */
  .tag.fuzzy{background:#f3eeff;color:#6544b0}
  .tag.exact{background:#eaf7f1;color:#0e8f6f}
  .ev{font-size:10.5px;color:#6544b0}
  .top-pick{background:#fff8e8;border:1px solid #f0dca8;border-radius:8px;padding:9px 13px;
    margin:10px 0;font-size:12.5px;line-height:1.8}
  .top-pick b{color:#a33}
  .news-item{padding:8px 0;border-bottom:1px dashed var(--line);font-size:12.5px}
  .news-item:last-child{border-bottom:none}
  .news-item .src{font-size:10.5px;color:var(--muted);margin-left:6px}
  .plat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(225px,1fr));gap:12px}
  .plat-col{border:1px solid var(--line);border-radius:9px;padding:10px 12px;background:#fbfcfe}
  .plat-col h4{margin:0 0 8px;font-size:12.5px;display:flex;align-items:center;gap:6px}
  .plat-col .ln{display:flex;gap:7px;font-size:11.5px;padding:4px 0;border-bottom:1px dashed var(--line);line-height:1.5}
  .plat-col .ln:last-child{border-bottom:none}
  .plat-col .ln .r{color:#d2601a;font-weight:600;min-width:22px;font-variant-numeric:tabular-nums}
  .plat-col .ln .k{flex:1;word-break:break-all}
  .cov{display:grid;grid-template-columns:repeat(auto-fit,minmax(205px,1fr));gap:8px}
  .cov .u{border:1px solid var(--line);border-radius:8px;padding:9px 11px;background:#fbfcfe}
  .cov .u .n{font-weight:600;font-size:12px}
  .cov .u .m{font-size:11px;color:var(--muted);margin-top:3px;line-height:1.5}
  .cov .u .s{font-size:10.5px;margin-top:5px}
  .st-scheduled_skip{color:#8a94a8}
  .ck{margin-top:12px;padding:9px 12px;border-radius:8px;background:#f2f8ff;
      border:1px solid #cfe1fb;font-size:12px;line-height:1.7}
  /* 起号准备工作台 / 主持人智库 */
  .bench{border:1px solid #c9dbff;background:linear-gradient(180deg,#f7faff,#fff);
         border-radius:11px;padding:16px 18px;margin-bottom:16px}
  .bench .head{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:12px}
  .bench h2 .sub{font-size:11px;color:var(--muted);font-weight:400}
  .poscard{background:#eef3ff;border:1px solid #cfe1fb;border-radius:9px;padding:12px 14px;margin-bottom:14px;font-size:12.5px;line-height:1.7}
  .poscard b{color:#1c2c4c}
  .poscard .formats{margin-top:6px}
  .poscard .fmt{display:inline-block;margin:0 5px 5px 0;padding:2px 9px;border-radius:20px;
                background:#fff;border:1px solid #cfe1fb;color:#335;font-size:11px}
  .bench-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
  @media(max-width:1000px){.bench-grid{grid-template-columns:1fr}}
  .kb-col h3{font-size:12.5px;margin:0 0 9px;color:#2a3a5c;display:flex;align-items:center;gap:7px}
  .kb-col h3 .cnt{font-size:11px;color:var(--muted);font-weight:400}
  .asset{display:flex;gap:10px;align-items:flex-start;padding:8px 0;border-bottom:1px dashed var(--line);font-size:12.5px}
  .asset:last-child{border-bottom:none}
  .asset .st{font-weight:650;color:#2f5bea;font-size:15px;min-width:18px;text-align:center}
  .asset .nm{font-weight:600;color:#1c2c4c}
  .asset .mp{font-size:11px;color:var(--muted)}
  .fw{border:1px solid var(--line);border-radius:8px;padding:9px 11px;margin-bottom:8px;font-size:12px;line-height:1.6}
  .fw .nm{font-weight:600;color:#1c2c4c}
  .fw .hk{color:#a33;font-style:italic}
  .fw .stxt{color:#5b6782;margin-top:2px}
  .fw .skel{margin-top:5px;padding:6px 9px;border-radius:6px;background:#eef7ee;border:1px solid #cfead0;
            color:#1f5132;font-size:11px;line-height:1.6;white-space:pre-wrap}
  .skel b{color:#163d24}
  .kb-item{padding:8px 0;border-bottom:1px dashed var(--line);font-size:12.5px}
  .kb-item:last-child{border-bottom:none}
  .kb-item .tt{font-weight:600;color:#1c2c4c}
  .kb-item .meta{font-size:11px;color:#5b6782;margin-top:3px;line-height:1.6}
  .kb-item .ang{color:#5b6782;font-size:11.5px;margin-top:3px;line-height:1.6}
  .kb-item .script{margin-top:6px;padding:8px 10px;border-radius:7px;background:#eef7ee;
            border:1px solid #cfead0;color:#1f5132;font-size:11px;line-height:1.65;white-space:pre-wrap}
  .kb-item .script b{color:#163d24}
  .kb-item .badge{font-size:10px;padding:1px 7px;border-radius:20px;margin-right:6px}
  .badge.ready{background:#e7f7ec;color:#0e8f6f}
  .badge.validated{background:#eef2fb;color:#2f5bea}
  .badge.incubating{background:#f3f0fb;color:#7a5bb0}
  .promote{float:right;border:1px solid #cfe1fb;background:#fff;border-radius:6px;padding:2px 9px;
           font-size:11px;color:#2f5bea;cursor:pointer}
  .promote:hover{background:#eef3ff}
  .cklist{margin-top:14px;background:#f4f8ff;border:1px solid #cfe1fb;border-radius:9px;padding:12px 14px}
  .cklist h3{font-size:13px;margin:0 0 8px;color:#1c2c4c}
  .cklist .row{display:flex;align-items:center;gap:9px;font-size:12.5px;padding:4px 0}
  .cklist .box{width:16px;height:16px;border-radius:4px;display:inline-flex;align-items:center;
               justify-content:center;font-size:11px;flex:0 0 auto}
  .cklist .box.ok{background:#12a594;color:#fff}
  .cklist .box.no{background:#eee;color:#aaa;border:1px solid #ddd}
  .cklist .bar{height:6px;background:#e6ecf7;border-radius:4px;overflow:hidden;margin-top:8px}
  .cklist .bar>i{display:block;height:100%;background:linear-gradient(90deg,#3aa,#2f5bea)}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>创作者情报工作站 <small>V1.0 · 实时创作者数据库看板</small></h1>
    <div class="meta"><span class="dot"></span><span id="meta">加载中…</span>
      &nbsp;<button onclick="load(true)">立即刷新</button></div>
  </header>

  <div class="freshbar" id="freshbar"><span>数据新鲜度加载中…</span></div>

  <section class="panel brief">
    <h2>今日情报简报 <span class="sub" id="brief-sub">为你收敛：现在该追什么、能改编成什么</span></h2>
    <div id="brief"></div>
  </section>

  <section class="bench">
    <div class="head">
      <h2>起号准备工作台 <span class="sub">单人自拍口播智库 · 数据自动沉淀，起号前就把「能拍的东西」攒好</span></h2>
      <span class="meta" id="kb-summary"></span>
    </div>
    <div id="knowledge"></div>
    <div id="smzdm"></div>
  </section>

  <section class="cards" id="cards"></section>

  <div class="grid2">
    <section class="panel">
      <h2>热点异动 <span class="sub">对比最近两轮 · 新进 / 上升 / 退出榜</span></h2>
      <div id="movements"></div>
    </section>
    <section class="panel">
      <h2>关注词追踪 <span class="sub">config/keywords.json · 命中平台数与跨轮走势</span></h2>
      <div id="keywords" class="scroll"></div>
    </section>
  </div>

  <section class="panel">
    <h2>全网热榜总览 <span class="sub">多平台同轮主榜 · 抖音为第一入口 · 每 2 分钟更新</span></h2>
    <div id="trends"></div>
  </section>

  <div class="grid2">
    <section class="panel">
      <h2>抖音官方热点榜 <span class="sub">TOP 15 · 按榜位 · 免登录公开接口</span></h2>
      <div id="douyin" class="scroll"></div>
    </section>
    <section class="panel">
      <h2>新闻线索 · 可点原文 <span class="sub">网易热点 / 今日头条 · 带出处与发布时间，口播/故事向可直接核实</span></h2>
      <div id="news" class="scroll"></div>
    </section>
  </div>

  <div class="grid2">
    <section class="panel">
      <h2>B站作品榜 <span class="sub">TOP 15 · 按播放量 · 含创作者与全量指标</span></h2>
      <div id="works" class="scroll"></div>
    </section>
    <section class="panel">
      <h2>数据源覆盖 <span class="sub">分级调度、末轮状态与合规口径</span></h2>
      <div id="coverage"></div>
      <div id="crosscheck"></div>
    </section>
  </div>

  <div class="grid2">
    <section class="panel">
      <h2>候选选题机会 <span class="sub">跨平台共振聚合 · 模糊匹配已降级标注 · rule_engine_v2</span></h2>
      <div id="topics" class="scroll"></div>
    </section>
    <section class="panel">
      <h2>B站创作者榜 <span class="sub">TOP 10 · 按本轮全部上榜作品总播放（统一聚合口径）</span></h2>
      <div id="creators" class="scroll"></div>
    </section>
  </div>

  <div class="grid2">
    <section class="panel">
      <h2>增长最快作品 <span class="sub">最近 12 轮快照播放增量</span></h2>
      <div id="growth" class="scroll"></div>
    </section>
    <section class="panel">
      <h2>采集时序 <span class="sub">每轮真实新增条目数</span></h2>
      <div id="timeline"></div>
    </section>
  </div>

  <section class="panel">
    <h2>数据导出 <span class="sub">整表 CSV（UTF-8 BOM，Excel 可直接打开）/ 全量 JSON · 均为真实采集记录</span></h2>
    <div id="exports"></div>
  </section>

  <details class="ops">
    <summary>系统运行监控 <span class="sub">数据连续性 / 数据源健康 / 维护留痕（运维用，点击展开）</span></summary>
    <section class="panel">
      <h2>数据连续性 <span class="sub">系统级 cron 自愈监控 · 一旦中断在这里红字告警</span></h2>
      <div id="continuity"></div>
    </section>
    <section class="panel">
      <h2>数据源健康 <span class="sub">最近一轮采集状态 + 历史成功率（collection_runs 审计）</span></h2>
      <div id="health"></div>
    </section>
  </details>

  <footer id="footer"></footer>
</div>

<script>
const EMBED = __DATA_JSON__;
const PC = {douyin:'#161823',weibo:'#f5a623',baidu:'#3385ff',bilibili:'#fb7299',
            toutiao:'#e8503a',zhihu:'#0a68e0',netease:'#c8302e'};
const PN = {douyin:'抖音',weibo:'微博',baidu:'百度',bilibili:'B站',
            toutiao:'头条',zhihu:'知乎',netease:'网易'};
/* 平台展示优先级：抖音是主入口，其余按情报价值排序 */
const PO = ['douyin','weibo','baidu','toutiao','netease','zhihu','bilibili'];
function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,function(c){
  return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
function n(v){if(v==null)return '—';return Number(v).toLocaleString('zh-CN');}
function short(s,k){s=String(s==null?'':s);return s.length>k?s.slice(0,k)+'…':s;}
function bar(v,max,cls){var p=max>0?Math.max(2,Math.round(v/max*100)):2;
  return '<div class="bar '+(cls||'')+'"><i style="width:'+p+'%"></i></div>';}

function drawCards(o){
  var c=o.counts||{}, items=[
    ['活跃数据源', o.sources_registered], ['覆盖平台', o.platform_count],
    ['今日热点记录', o.today_trends], ['创作者', c.creators],
    ['作品', c.works], ['作品快照', c.work_snapshots],
    ['候选选题', c.topic_opportunities], ['采集轮次', o.rounds]
  ];
  document.getElementById('cards').innerHTML = items.map(function(x){
    return '<div class="card"><div class="num">'+n(x[1]||0)+'</div><div class="lab">'+esc(x[0])+'</div></div>';
  }).join('');
  document.getElementById('meta').textContent =
    '最近采集 '+(o.last_run||'—')+' · 生成于 '+(o.generated_at||'—')+' · 每 15s 自动刷新';
}

/* ---- 实时新鲜度条：页面自己按秒走，不信任可能冻结的后端快照 ---- */
var FRESH={age:null,next:null,iv:120,thr:600,last:null,embed:false,t0:0};
function mmss(s){s=Math.max(0,Math.round(s));return s<60?s+' 秒':Math.floor(s/60)+' 分 '+(s%60)+' 秒';}
function drawFreshbar(o){
  FRESH.age=o.data_age_seconds; FRESH.next=o.next_update_seconds;
  FRESH.iv=o.interval_seconds||120; FRESH.thr=o.freshness_threshold_seconds||600;
  FRESH.last=o.last_run; FRESH.embed=!!EMBED; FRESH.t0=Date.now();
  FRESH.tick();
}
FRESH.tick=function(){
  var el=document.getElementById('freshbar'); if(!el)return;
  var age=FRESH.age,next=FRESH.next,d=0;
  if(!FRESH.embed && age!=null){
    d=(Date.now()-FRESH.t0)/1000; age+=d; next-=d;
    if(next<0) next=FRESH.iv+(next%FRESH.iv);
  }
  var cls='',label='',color='#12a594';
  if(age==null){cls='bad';label='无采集记录';color='#d94b4b';}
  else if(age>FRESH.thr){cls='bad';label='采集中断';color='#d94b4b';}
  else if(age>180){cls='warn';label='数据略有滞后';color='#e0842a';}
  else label='数据实时';
  el.className='freshbar'+cls;
  var h='<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:'+color+'"></span>'+
    '<b style="color:'+color+'">'+label+'</b><span class="sep">|</span>'+
    '最近采集 <b>'+esc((FRESH.last||'—').slice(11,19)||FRESH.last||'—')+'</b>'+
    '<span class="sep">|</span>距今 <span class="cd">'+(age==null?'—':mmss(age))+'</span>';
  if(FRESH.embed){
    h+='<span class="sep">|</span>离线快照，数据定格于此；启动看板服务可获得 2 分钟级实时更新';
  }else{
    h+='<span class="sep">|</span>下次采集约 <span class="cd">'+(next==null?'—':mmss(next))+'</span>后'+
       '<span class="sep">|</span>采集周期 '+Math.round(FRESH.iv/60*10)/10+' 分钟 · 页面每 15s 刷新';
  }
  el.innerHTML=h;
};
setInterval(function(){if(FRESH.t0)FRESH.tick();},1000);

function plat(p){return '<span class="pl">'+esc(PN[p]||p)+'</span>';}
function drawBrief(b){
  var el=document.getElementById('brief');
  if(!b||!b.has_data){el.innerHTML='<div class="empty">'+esc((b&&b.note)||'暂无情报数据')+'</div>';return;}
  var sub=document.getElementById('brief-sub');
  if(sub) sub.textContent='面向「'+esc(b.account||'你的账号')+'」· '+esc(b.style||'通用')+
    ' · 共振 '+n(b.exact_resonance||0)+' 精确 / '+n(b.fuzzy_resonance||0)+' 模糊 · 数据截至 '+esc(b.at||'—');
  // 一句话结论：优先引用「整句精确共振」；没有精确共振才用模糊簇，
  // 避免把推断当最强信号推给用户（模糊簇在列表里仍按平台数展示并标注）。
  var r0all=b.resonance||[];
  var r0=r0all.filter(function(x){return x.match==='exact';})[0]||r0all[0];
  var u0=b.rising&&b.rising[0];
  var go='';
  if(r0){
    go='现在最值得先追 <b>「'+esc(r0.keyword)+'」</b>：'+r0.platforms.map(function(p){return PN[p]||p;}).join(' / ')+
       ' 多平台同时上榜（最佳榜位 '+n(r0.best_rank)+'）'+
       (r0.match==='fuzzy'?'（模糊共现推断，需人工核实）':'（整句精确共振）');
    if(u0) go+='；另外 <b>「'+esc(u0.keyword)+'」</b> 上一轮到现在上升 <b>+'+n(u0.delta)+' 位</b>，正处切入窗口';
    go+='。';
  }else if(u0){
    go='本轮暂无跨平台共振话题；<b>「'+esc(u0.keyword)+'」</b> 上升 <b>+'+n(u0.delta)+' 位</b>，可作为切入窗口观察。';
  }else{
    go='本轮以单平台热点为主，可直接看下方「抖音热点」与「可改编选题」。';
  }
  var h='<div class="go">'+go+'</div>';
  var tp=b.top_picks||[];
  if(tp.length){
    h+='<div class="top-pick"><b>今天建议优先拍这 '+tp.length+' 个：</b>'+
      '<ol style="margin:6px 0 0 18px;padding:0">'+tp.map(function(x){
        return '<li style="margin:4px 0"><b>「'+esc(x.keyword)+'」</b> '+
          '<span class="cat">'+esc(x.category||'')+'</span> '+plat(x.platform)+
          '<div class="ang">'+esc(x.angle||'')+'</div></li>';
      }).join('')+'</ol></div>';
  }

  // 左列：跨平台共振 + 上升/新进
  var L='<h3>跨平台共振 · 真热点 <span class="cnt">多平台同时上榜</span></h3>';
  var rs=b.resonance||[];
  if(!rs.length){L+='<div class="empty">本轮无跨平台共振话题</div>';}
  rs.slice(0,4).forEach(function(x){
    var mt=(x.match==='fuzzy')
      ? '<span class="tag fuzzy" title="由共享词片段推断，需人工核实">模糊</span> '+
        '<span class="ev">共现「'+esc((x.evidence||[]).join('/'))+'」</span> '
      : '<span class="tag exact" title="整句归一化完全一致">精确</span> ';
    L+='<div class="row">'+mt+'<span class="kw">'+esc(x.keyword)+'</span> '+
       '<span class="rk">最佳 #'+n(x.best_rank)+'</span>'+
       x.platforms.map(plat).join('')+
       '<div class="ang">'+n(x.platform_count||(x.platforms||[]).length)+' 个平台共振 · 热度峰值 '+n(x.max_heat)+'</div></div>';
  });
  L+='<h3 style="margin-top:16px">正在起势 · 切入窗口 <span class="cnt">对比上一轮</span></h3>';
  var up=b.rising||[], fr=b.fresh||[];
  if(!up.length&&!fr.length){L+='<div class="empty">需至少两轮数据</div>';}
  up.slice(0,3).forEach(function(x){
    L+='<div class="row"><span class="up">↑+'+n(x.delta)+'</span> <span class="kw">'+esc(x.keyword)+'</span>'+
       ' <span class="rk">现 #'+n(x.rank)+'</span>'+plat(x.platform)+'</div>';
  });
  fr.slice(0,2).forEach(function(x){
    L+='<div class="row"><span class="tag new">新</span> <span class="kw">'+esc(x.keyword)+'</span>'+
       ' <span class="rk"># '+n(x.rank)+'</span>'+plat(x.platform)+'</div>';
  });

  // 右列：故事向可改编选题
  var R='<h3>可改编成故事 · 带切入角度 <span class="cnt">按你的赛道画像匹配</span></h3>';
  var pk=b.picks||[];
  if(!pk.length){R+='<div class="empty">本轮热榜未命中赛道信号词（可在 config/creator_profile.json 调整）</div>';}
  pk.slice(0,5).forEach(function(x){
    var flags=(x.resonance?'<span class="tag hot">共振</span> ':'')+(x.rising?'<span class="tag new">上升</span> ':'');
    R+='<div class="row">'+flags+'<span class="cat">'+esc(x.category)+'</span>'+
       '<span class="kw">'+esc(x.keyword)+'</span> <span class="rk">#'+n(x.rank)+'</span>'+plat(x.platform)+
       '<div class="ang">'+esc(x.angle)+'</div></div>';
  });

  // 抖音榜（主入口）
  var D='<h3 style="margin-top:16px">抖音热点（主入口） <span class="cnt">官方榜 · 免登录</span></h3>';
  (b.douyin_top||[]).slice(0,5).forEach(function(x){
    D+='<div class="row"><span class="rk">#'+n(x.rank)+'</span> <span class="kw">'+esc(x.keyword)+'</span>'+
       '<span class="ang" style="display:inline">· 热度 '+n(x.heat)+'</span></div>';
  });

  h+='<div class="cols"><div>'+L+'</div><div>'+R+D+'</div></div>';
  h+='<div class="disc">'+esc(b.disclaimer||'')+'。改编角度仅为选题方向的规则建议，具体事实请以原始链接与官方信息核实。</div>';
  el.innerHTML=h;
}

var STN={success:'成功',partial:'部分',blocked:'失败',scheduled_skip:'按期间隔跳过',needs_auth:'待授权'};
function drawHealth(o, stats){
  var h=o.health||[], labels=o.source_labels||{}, order=o.source_order||[];
  var st={}; (stats||[]).forEach(function(s){st[s.source]=s;});
  if(!h.length){document.getElementById('health').innerHTML='<div class="empty">暂无采集记录</div>';return;}
  var map={}; h.forEach(function(r){map[r.source]=r;});
  var seq=order.filter(function(s){return map[s];}).concat(h.map(function(r){return r.source;}).filter(function(s){return order.indexOf(s)<0;}));
  var html='<table><thead><tr><th>数据源</th><th>状态</th><th class="r">新增 / 处理</th><th class="r">累计轮次</th><th class="r">成功率</th><th>说明</th></tr></thead><tbody>';
  seq.forEach(function(s){var r=map[s]; if(!r)return; var q=st[s]||{};
    var rate=q.runs?Math.round(q.ok/q.runs*100)+'%':'—';
    html+='<tr><td>'+esc(labels[s]||s)+'</td><td class="st-'+esc(r.status)+'">'+esc(STN[r.status]||r.status)+'</td>'+
      '<td class="r">'+n(r.items_collected)+' / '+n(r.items_seen)+'</td><td class="r">'+n(q.runs)+'</td>'+
      '<td class="r">'+esc(rate)+'</td><td class="heat">'+esc(short(r.note,60))+'</td></tr>';
  });
  document.getElementById('health').innerHTML=html+'</tbody></table>'+
    '<div class="meta" style="margin-top:7px">成功率分母不含「按期间隔跳过」；'+
    '新增 &lt; 处理 属正常（同一作品被多个榜单重复采到，快照去重后不重复入库）。</div>';
}

function drawSmzdm(s){
  var el=document.getElementById('smzdm');
  if(!el) return;
  if(!s||!s.has_data){
    el.innerHTML='<div class="panel"><h2>快消选品情报 <span class="sub">卖什么 · 数据驱动</span></h2><div class="empty">选品数据采集中…</div></div>';
    return;
  }
  var h='<div class="panel"><h2>快消选品情报 <span class="sub">卖什么 · 数据驱动 · 什么值得买实时</span></h2>';
  h+='<div class="grid2">';
  h+='<div><h3>纸品 <span class="cnt">'+n(s.total_paper)+' 个在榜选品</span></h3>';
  if(!s.paper.length) h+='<div class="empty">暂无</div>';
  s.paper.forEach(function(x){ h+=smzdmItem(x); });
  h+='</div>';
  h+='<div><h3>预包装食品 <span class="cnt">'+n(s.total_food)+' 个在榜选品</span></h3>';
  if(!s.food.length) h+='<div class="empty">暂无</div>';
  s.food.forEach(function(x){ h+=smzdmItem(x); });
  h+='</div>';
  h+='</div>';
  h+='<div class="brief disc" style="margin-top:12px">'+esc(s.disclaimer||'')+'</div></div>';
  el.innerHTML=h;
}
function smzdmItem(x){
  var u=x.url?(' <a href="'+esc(x.url)+'" target="_blank" class="lk">看商品 ↗</a>'):'';
  var cat=(x.cat4||x.cat3)?(' <span class="tag2">'+esc(x.cat4||x.cat3)+'</span>'):'';
  return '<div class="kb-item"><span class="tt">'+esc(x.title)+'</span>'+
    '<div class="meta">'+cat+u+'</div></div>';
}

function drawKnowledge(k){
  var el=document.getElementById('knowledge');
  if(!el) return;
  if(!k||!k.has_data){
    el.innerHTML='<div class="empty">智库沉淀中（需先有几轮热榜数据）…</div>';
    return;
  }
  // 顶部汇总
  var cc=k.counts||{};
  document.getElementById('kb-summary').textContent =
    '常青 '+n(cc.validated||0)+' · 孵化 '+n(cc.incubating||0)+' · 待拍 '+n(cc.ready||0)+
    ' · 累计沉淀 '+n(k.total||0)+' 条';

  // 定位卡
  var pos='<div class="poscard">';
  pos+='<b>你是谁：</b>'+(k.identity||'—')+'<br>';
  pos+='<b>一句话：</b>'+(k.one_liner||'—')+'<br>';
  pos+='<b>起号定位草稿：</b>'+(k.positioning_draft||'—');
  if((k.suitable_formats||[]).length){
    pos+='<div class="formats"><b>适合你的形式：</b>';
    k.suitable_formats.forEach(function(f){pos+='<span class="fmt">'+esc(f)+'</span>';});
    pos+='</div>';
  }
  pos+='</div>';

  // 左列：口播能力库 + 口播框架库（含脚本骨架）
  var L='<h3>你的口播能力库 <span class="cnt">真人出镜 · 口语化 · 观点 / 故事</span></h3>';
  (k.assets||[]).forEach(function(a){
    L+='<div class="asset"><div class="st">'+n(a.strength)+'</div><div>'+
       '<div class="nm">'+esc(a.name)+'</div>'+
       '<div class="mp">→ '+esc(a.maps_to||'')+'</div></div></div>';
  });
  L+='<h3 style="margin-top:16px">可复用口播框架库 <span class="cnt">含脚本骨架 · 起号前先练熟</span></h3>';
  (k.frameworks||[]).forEach(function(f){
    L+='<div class="fw"><div class="nm">'+esc(f.name)+'</div>'+
       '<div class="stxt">'+esc(f.structure||'')+'</div>'+
       (f.hook_example?'<div class="hk">开场示例：'+esc(f.hook_example)+'</div>':'')+
       (f.skeleton?'<div class="skel">脚本骨架：'+esc(f.skeleton)+'</div>':'')+'</div>';
  });

  // 右列：今日口播选题(quick pick) + 常青/孵化选题库 + 准备清单
  var pk=(k.picks||[]);
  var R='<h3>今日口播选题 <span class="cnt">常青里 durability 最高 · 起号前就能反复拍</span></h3>';
  if(!pk.length) R+='<div class="empty">常青选题积累中（话题需反复出现才会沉淀）</div>';
  pk.slice(0,6).forEach(function(x){ R+=kbItem(x,true); });
  R+='<h3 style="margin-top:16px">常青选题库 <span class="cnt">跨轮/跨平台反复出现 · 起号后可反复做</span></h3>';
  var ev=k.evergreen||[];
  if(!ev.length) R+='<div class="empty">暂无常青选题（话题需反复出现才会沉淀为常青）</div>';
  ev.slice(0,8).forEach(function(x){ R+=kbItem(x,true); });
  R+='<h3 style="margin-top:16px">孵化中选题 <span class="cnt">本轮命中 · 待验证</span></h3>';
  var inc=k.incubating||[];
  if(!inc.length) R+='<div class="empty">本轮热榜未命中你的赛道信号词</div>';
  inc.slice(0,6).forEach(function(x){ R+=kbItem(x,false); });

  // 准备清单
  var ck=(k.checklist||[]);
  var ckH='<div class="cklist"><h3>起号准备清单 <span class="meta" style="font-weight:400">'+n(k.checklist_done||0)+'/'+ck.length+' 完成</span></h3>';
  ck.forEach(function(x){
    ckH+='<div class="row"><span class="box '+(x.done?'ok':'no')+'">'+(x.done?'✓':'')+'</span>'+
         '<span style="color:'+(x.done?'#1b2333':'#7b879c')+'">'+esc(x.item)+'</span></div>';
  });
  var pct = ck.length? Math.round((k.checklist_done||0)/ck.length*100):0;
  ckH+='<div class="bar"><i style="width:'+pct+'%"></i></div></div>';

  el.innerHTML = pos + '<div class="bench-grid"><div>'+L+'</div><div>'+R+ckH+'</div></div>' +
    '<div class="brief disc" style="margin-top:14px">'+esc(k.disclaimer||'')+'</div>';
}

function kbItem(x, canPromote){
  var badge = x.status==='ready' ? '<span class="badge ready">待拍</span>'
            : x.status==='validated' ? '<span class="badge validated">常青</span>'
            : '<span class="badge incubating">孵化</span>';
  var prom = (canPromote && x.status!=='ready')
    ? '<span class="promote" onclick="kbPromote('+x.id+',this)">标为待拍 ▸</span>' : '';
  var plats = (x.platforms||[]).map(function(p){return (PN[p]||p);}).join('/');
  var scriptHtml = x.script ? '<div class="script">'+esc(x.script).replace(/\n/g,'<br>')+'</div>' : '';
  return '<div class="kb-item">'+prom+badge+'<span class="tt">'+esc(x.display_title||x.title||'')+'</span>'+
    '<div class="meta">'+esc(x.category||'')+' · 命中 '+esc(x.fw_name||'')+
    ' · 在榜 '+n(x.rounds_seen)+' 轮 / '+n((x.platforms||[]).length)+' 平台 · durability '+n(x.durability)+
    (plats?' · '+esc(plats):'')+'</div>'+
    (x.angle?'<div class="ang">'+esc(x.angle)+'</div>':'')+
    scriptHtml+'</div>';
}

function kbPromote(id, elm){
  if(EMBED){ alert('离线快照不支持操作，请启动看板服务'); return; }
  elm.textContent='提交中…';
  fetch('/api/kb/'+id+'/promote',{method:'POST'}).then(function(r){
    return r.json();
  }).then(function(j){
    if(j.ok){ elm.textContent='已待拍 ✓'; elm.style.background='#e7f7ec'; }
    else { elm.textContent='标为待拍 ▸'; alert(j.error||'操作失败'); }
  }).catch(function(e){ elm.textContent='标为待拍 ▸'; });
}

function drawDouyin(t){
  var list=(t&&t.douyin)||[];
  if(!list.length){document.getElementById('douyin').innerHTML='<div class="empty">暂无数据</div>';return;}
  var max=Math.max.apply(null,list.map(function(x){return x.heat_value||0;}))||1;
  var h='<table><thead><tr><th style="width:34px">#</th><th>热点词</th><th>标签</th><th class="r">热度</th></tr></thead><tbody>';
  list.forEach(function(x){
    h+='<tr><td>'+n(x.rank)+'</td><td><div>'+esc(x.keyword)+'</div>'+bar(x.heat_value||0,max,'dy')+'</td>'+
       '<td><span class="tag hot">'+esc(x.tag||'—')+'</span></td><td class="r heat">'+n(x.heat_value)+'</td></tr>';
  });
  document.getElementById('douyin').innerHTML=h+'</tbody></table>';
}

function drawWorks(list){
  if(!list||!list.length){document.getElementById('works').innerHTML='<div class="empty">暂无数据</div>';return;}
  var max=list[0].play_count||1;
  var h='<table><thead><tr><th>作品</th><th>创作者 / 分区</th><th class="r">播放</th><th class="r">点赞</th></tr></thead><tbody>';
  list.forEach(function(x){
    h+='<tr><td><a href="'+esc(x.url||'#')+'" target="_blank" rel="noopener">'+esc(short(x.title,26))+'</a>'+bar(x.play_count||0,max)+'</td>'+
       '<td>'+esc(short(x.creator,12))+'<div class="heat" style="font-size:11px">'+esc(x.category||'—')+'</div></td>'+
       '<td class="r heat">'+n(x.play_count)+'</td><td class="r heat">'+n(x.like_count)+'</td></tr>';
  });
  document.getElementById('works').innerHTML=h+'</tbody></table>';
}

function drawTrends(t){
  var all=PO.filter(function(p){return t&&t[p]&&t[p].length;});
  Object.keys(t||{}).forEach(function(p){if(all.indexOf(p)<0&&t[p]&&t[p].length)all.push(p);});
  if(!all.length){document.getElementById('trends').innerHTML='<div class="empty">暂无数据</div>';return;}
  var h='<div class="plat-grid">';
  all.forEach(function(p){
    var list=t[p];
    h+='<div class="plat-col"><h4><i class="pill" style="background:'+(PC[p]||'#888')+'"></i>'+
       esc(PN[p]||p)+' <span style="font-weight:400;color:#7b879c">TOP '+Math.min(list.length,8)+'</span></h4>';
    list.slice(0,8).forEach(function(x){
      var hv=(x.heat_available===0)?'—':n(x.heat_value);
      h+='<div class="ln"><span class="r">'+n(x.rank)+'</span><span class="k">'+esc(x.keyword)+'</span>'+
         '<span class="heat" style="font-size:10.5px;white-space:nowrap">'+hv+'</span></div>';
    });
    h+='</div>';
  });
  document.getElementById('trends').innerHTML=h+'</div>'+
    '<div class="meta" style="margin-top:9px">按平台主榜展示（抖音取官方热点榜，非镜像校验源）；'+
    '网易 / 知乎不公开热度，以「—」表示，按榜位阅读。</div>';
}

function drawNews(x){
  var el=document.getElementById('news');
  if(!x||!x.has_data||!x.items||!x.items.length){el.innerHTML='<div class="empty">暂无新闻线索</div>';return;}
  var h='<div class="meta" style="margin-bottom:8px">共 '+n(x.items.length)+' 条 · 采集于 '+esc(x.at||'—')+
        ' · 命中赛道信号词的已标注「可改编」</div>';
  x.items.forEach(function(it){
    var link=it.url?'<a href="'+esc(it.url)+'" target="_blank" rel="noopener">'+esc(it.keyword)+'</a>':esc(it.keyword);
    h+='<div class="news-item"><span class="tag">'+esc(PN[it.platform]||it.platform)+'</span> '+
       '<b style="font-weight:600">#'+n(it.rank)+'</b> '+link+
       (it.media?'<span class="src">'+esc(it.media)+'</span>':'')+
       (it.publish_time?'<span class="src">'+esc(it.publish_time)+'</span>':'')+
       (it.categories&&it.categories.length?'<div class="ang"><span class="tag hot">可改编</span> '+esc(it.categories.join(' / '))+'</div>':'')+
       '</div>';
  });
  el.innerHTML=h;
}

function drawCoverage(c){
  var el=document.getElementById('coverage');
  if(!c||!c.sources||!c.sources.length){el.innerHTML='<div class="empty">数据源目录未生成</div>';return;}
  var h='<div class="meta" style="margin-bottom:9px">'+n(c.source_count)+' 源 / '+n(c.platform_count)+
    ' 平台 · 末轮实跑 '+n(c.ran)+' · 分级跳过 '+n(c.skipped)+
    ' · <span class="st-scheduled_skip">跳过 = 未到该源间隔，不是失败</span></div><div class="cov">';
  c.sources.forEach(function(u){
    var r=u.run, sc='本轮无记录';
    if(r){
      sc='本轮 <span class="st-'+esc(r.status)+'">'+esc(r.status)+'</span>'+
         (r.status==='scheduled_skip'?'':' · 新增 '+n(r.items_collected)+' / 处理 '+n(r.items_seen));
    }
    h+='<div class="u"><div class="n"><i class="pill" style="background:'+(PC[u.platform]||'#888')+'"></i>'+
       esc(u.label||u.name)+'</div>'+
       '<div class="m">'+esc(u.tier||'')+' · 每 '+n(u.interval_s)+'s · '+esc(u.provides||'')+'</div>'+
       '<div class="s">'+sc+'</div></div>';
  });
  el.innerHTML=h+'</div>';
}

function drawCrosscheck(k){
  var el=document.getElementById('crosscheck'); if(!el)return;
  if(!k||!k.has_data){el.innerHTML='<div class="meta">双源校验待数据</div>';return;}
  var ok=k.head_match&&k.jaccard>=0.6;
  el.className='ck';
  el.innerHTML='<b>抖音双源交叉校验</b>：主榜 <code>douyin_hot</code> 与官方镜像 <code>douyin_ies</code> '+
    'TOP'+n(k.top_n)+' 重合 '+n(k.overlap)+'/'+n(k.top_n)+'（Jaccard '+n(Math.round(k.jaccard*100))+'%），'+
    '榜首'+(k.head_match?'一致':'不一致')+' → <b style="color:'+(ok?'#0e8f6f':'#a33')+'">'+
    (ok?'两条独立链路互相印证，抖音榜可信':'需留意单接口差异')+'</b>'+
    '<div class="meta" style="margin-top:4px">主榜「'+esc(k.head_a||'—')+'」 / 镜像「'+esc(k.head_b||'—')+'」</div>';
}

function drawCreators(list){
  if(!list||!list.length){document.getElementById('creators').innerHTML='<div class="empty">暂无数据</div>';return;}
  var max=list[0].total_views||1;
  var h='<table><thead><tr><th>创作者</th><th class="r">上榜作品</th><th class="r">总播放</th><th class="r">总点赞</th></tr></thead><tbody>';
  list.forEach(function(x){
    h+='<tr><td>'+esc(short(x.display_name,16))+bar(x.total_views||0,max)+'</td>'+
       '<td class="r">'+n(x.work_count)+'</td><td class="r heat">'+n(x.total_views)+'</td>'+
       '<td class="r heat">'+n(x.total_likes)+'</td></tr>';
  });
  document.getElementById('creators').innerHTML=h+'</tbody></table>';
}

function drawTopics(list){
  if(!list||!list.length){document.getElementById('topics').innerHTML='<div class="empty">暂无候选（跨平台命中后自动生成）</div>';return;}
  var h='<table><thead><tr><th style="width:44px">优先</th><th>候选选题</th><th>依据 / 差异化切入</th></tr></thead><tbody>';
  list.forEach(function(x){
    h+='<tr><td class="prio '+esc(x.priority)+'">'+esc(x.priority)+'</td>'+
       '<td>'+esc(x.title)+'</td><td class="heat" style="font-size:11.5px">'+esc(x.supply_analysis)+'<br>'+esc(short(x.differentiation,70))+'</td></tr>';
  });
  document.getElementById('topics').innerHTML=h+'</tbody></table>';
}

function drawGrowth(list){
  if(!list||!list.length){document.getElementById('growth').innerHTML='<div class="empty">当前仅 1 轮快照，调度器持续运行后此处将显示跨轮增长</div>';return;}
  var max=Math.max.apply(null,list.map(function(x){return Math.max(x.delta_view,0);}))||1;
  var h='<table><thead><tr><th>作品 / 创作者</th><th class="r">播放增量</th><th class="r">点赞增量</th><th class="r">轮数</th></tr></thead><tbody>';
  list.forEach(function(x){
    h+='<tr><td><a href="'+esc(x.url||'#')+'" target="_blank" rel="noopener">'+esc(short(x.title,22))+'</a>'+
       '<div class="heat" style="font-size:11px">'+esc(short(x.creator,14))+'</div>'+bar(Math.max(x.delta_view,0),max)+'</td>'+
       '<td class="r heat">+'+n(x.delta_view)+'</td><td class="r heat">+'+n(x.delta_like)+'</td>'+
       '<td class="r">'+n(x.rounds)+'</td></tr>';
  });
  document.getElementById('growth').innerHTML=h+'</tbody></table>';
}

function drawTimeline(list){
  if(!list||!list.length){document.getElementById('timeline').innerHTML='<div class="empty">暂无</div>';return;}
  var max=Math.max.apply(null,list.map(function(x){return x.total;}))||1;
  var h='<div class="tl">'+list.slice(-30).map(function(x){
    return '<div class="col" title="'+esc(x.run_at)+' · '+n(x.total)+' 条" style="height:'+Math.max(3,Math.round(x.total/max*100))+'%"></div>';
  }).join('')+'</div><div class="meta" style="margin-top:6px">共 '+list.length+' 轮 · 最近一轮入库 '+
    n(list[list.length-1].total)+' 条 · '+esc(list[list.length-1].run_at)+'</div>';
  document.getElementById('timeline').innerHTML=h;
}

function drawFooter(o){
  var pd=(o.platform_distribution||[]).map(function(x){
    return '<i class="pill" style="background:'+(PC[x.platform]||'#888')+'"></i>'+(PN[x.platform]||x.platform)+' '+n(x.n);
  }).join('　');
  document.getElementById('footer').innerHTML=
    '<b>合规与数据口径（第二轮优化后）</b><br>'+
    '本看板全部数字来自 <code>database/workstation.db</code> 的真实采集记录，无演示数据。'+
    '共 <b>'+n(o.sources_registered)+' 个数据源 / '+n(o.platform_count)+' 个平台</b>，均为<b>免登录、公开可访问</b>的官方榜单/热点接口：'+esc(pd)+'。<br>'+
    '不使用登录态、不绕过任何签名/验证码/反爬、不采集他人隐私数据；知乎热榜 API（401）、快手/小红书（需签名）等需授权入口一律不接入。'+
    '<code>comments</code> 表保持为空（评论含个人信息，按任务书不纳入）。<br>'+
    '<b>不确定性的显式标注：</b>跨平台共振分「精确」（整句一致）与「模糊」（共享词片段推断，已降级并标注需人工核实）；'+
    '网易/知乎不公开热度，用 <code>heat_available=0</code> 标记并显示「—」，不填 0 假装是真实热度。'+
    '<code>content_analysis</code> / <code>topic_opportunities</code> 为<b>规则引擎派生</b>（data_status=derived, confidence=low），需人工二次筛选。'+
    '创作者快照为「本轮全部上榜作品统一聚合」，公开榜单不提供粉丝数，故 follower_count 留空。<br>'+
    '<b>主持人智库（本轮新增）：</b>把命中你赛道信号词的热榜话题，按电台主持的声音叙事 / 访谈 / 长内容能力匹配，'+
    '自动沉淀为带「电台视角改编角度」的选题；同一话题跨轮 / 跨平台反复出现会自动晋升为「常青选题」，'+
    '形成越积累越值钱的智库（knowledge_base 表，不随 trends 7 天清理被丢弃）。<br>'+
    '调度：cron 每 2 分钟触发，热榜 120s / 作品 300s / 搜索 600s / 日更 3600s 分级采集；'+
    '抖音主榜与官方镜像榜双源交叉校验；页面每 15 秒刷新，新鲜度条按秒实时核对。';
}

function drawContinuity(c, m, q){
  c=c||{};
  var el=document.getElementById('continuity');
  if(c.healthy===null||c.healthy===undefined){
    el.innerHTML='<div class="empty">'+esc(c.note||'健康快照未生成')+'</div>'; return;
  }
  // 用「浏览器当前时间 - 末轮采集时间」实时重算滞后，不信任后端/快照里的
  // since_last_minutes 与 healthy（它们可能停在服务挂掉前的那一刻，显示假的绿）。
  // 这样静态快照、cron 停摆、服务挂掉时，打开页面看到的都是此刻真值。
  var live=null;
  if(c.last){
    var t=Date.parse(c.last);
    if(!isNaN(t)) live=(Date.now()-t)/60000;   // 距末轮的分钟数（含时区无关，绝对差）
  }
  var lim=(c.freshness_threshold_seconds||600)/60;   // 后端单一真源：10 分钟
  var ok = live!==null ? (live<=lim) : c.healthy;
  var color=ok?'#12a594':'#d94b4b', label=ok?'当前运行正常':'数据已停滞 / 采集中断';
  var gapTxt = live!==null ? n(Math.round(live*10)/10) : n(c.since_last_minutes);
  var h='<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px">'+
    '<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:'+color+'"></span>'+
    '<b style="color:'+color+'">'+label+'</b>'+
    '<span class="meta">页面实时核对 '+new Date().toLocaleTimeString('zh-CN')+'</span></div>'+
    '<div class="meta" style="margin-bottom:8px">累计 '+n(c.rounds)+' 轮 · 首轮 '+esc(c.first||'—')+
    ' · 末轮 '+esc(c.last||'—')+' · 距今已 '+gapTxt+' 分钟（阈值 '+n(lim)+' 分钟）· 最大间隔 '+n(c.max_gap_minutes)+' 分钟</div>'+
    (live!==null&&live>lim ? '<div style="background:#fff2f2;border:1px solid #f3c9c9;border-radius:7px;padding:9px 12px;color:#a33;margin-bottom:8px">'+
       '<b>数据已停滞 '+n(Math.round(live*10)/10)+' 分钟</b>（末轮 '+esc(c.last)+'）。'+
       '调度/服务可能已中断；热榜为实时数据，停滞期间无法回溯补造。请核查系统级 cron 与看板服务是否在运行。</div>' : '');
  if(c.gaps&&c.gaps.length){
    h+='<div style="background:#fff8e8;border:1px solid #f0dca8;border-radius:7px;padding:9px 12px;color:#8a6410">'+
       '<b>历史空窗 '+c.gaps.length+' 段（已发生、不可回溯补造；不影响当前运行）：</b>'+
       '<ul style="margin:6px 0 0 18px;padding:0">';
    c.gaps.forEach(function(g){
      h+='<li>'+esc(g.from)+' → '+esc(g.to)+'（'+n(g.minutes)+' 分钟）</li>';
    });
    h+='</ul></div>';
  }else{
    h+='<div class="meta">未检测到超过 '+n(lim)+' 分钟的空窗。</div>';
  }
  var recs=(m&&m.records)||[];
  if(recs.length){
    h+='<div style="margin-top:12px"><b style="font-size:12.5px">最近 '+recs.length+
       ' 次系统维护执行（系统级 cron，不依赖会话）</b>'+
       '<table style="margin-top:6px"><thead><tr><th>执行时刻</th><th>动作</th><th class="r">当时轮次</th></tr></thead><tbody>';
    recs.forEach(function(r){
      h+='<tr><td class="heat">'+esc(r.at)+'</td><td style="font-size:11.5px">'+
         esc((r.actions||[]).join(' · '))+'</td><td class="r">'+n(r.rounds)+'</td></tr>';
    });
    h+='</tbody></table></div>';
  }
  if(q && q.score!==undefined && q.score!==null){
    var qc=q.healthy?'#12a594':'#d94b4b';
    h+='<div style="margin-top:12px"><b style="font-size:12.5px">数据质量自检</b>'+
       ' <span style="color:'+qc+';font-weight:600">'+n(q.score)+' 分 / '+(q.healthy?'通过':'有问题')+'</span>'+
       '<div class="meta" style="margin-top:4px">检查于 '+esc(q.checked_at||'—')+
       ' · 错误 '+n((q.issues||[]).length)+' 项 · 提醒 '+n((q.warnings||[]).length)+' 项</div>';
    (q.issues||[]).forEach(function(x){h+='<div style="color:#d94b4b;font-size:12px">• '+esc(x)+'</div>';});
    (q.warnings||[]).forEach(function(x){h+='<div style="color:#a8791a;font-size:12px">• '+esc(x)+'</div>';});
    h+='</div>';
  }
  el.innerHTML=h;
}

function drawMovements(m){
  var el=document.getElementById('movements');
  if(!m||!m.has_data){el.innerHTML='<div class="empty">'+esc((m&&m.note)||'需至少 2 轮热榜数据')+'</div>';return;}
  var h='<div class="meta" style="margin-bottom:8px">'+esc(m.prev_round)+' → '+esc(m.cur_round)+
    ' · 新进 '+n(m.new_count)+' · 上升 '+n(m.rising_count)+' · 退出 '+n(m.gone_count)+' · 持续在榜 '+n(m.stayed)+'</div>';
  h+='<table><thead><tr><th class="mv-col">新进榜</th><th class="mv-col">上升最快</th><th class="mv-col">退出榜</th></tr></thead><tbody><tr>';
  function cell(list, kind){
    var s='<td class="mv-col">';
    if(!list||!list.length) return s+'<span class="meta">无</span></td>';
    list.slice(0,6).forEach(function(x){
      s+='<div style="margin-bottom:6px;font-size:12px"><span class="tag">'+esc(x.platform)+'</span> '+
         esc(short(x.keyword,14));
      if(kind==='rising') s+=' <span style="color:#d94b4b">↑'+n(x.delta)+'</span>';
      s+='<div class="meta" style="font-size:11px">榜位 '+n(x.rank)+
         (kind==='rising'?'（原 '+n(x.from_rank)+'）':'')+'</div></div>';
    });
    return s+'</td>';
  }
  h+=cell(m.new,'new')+cell(m.rising,'rising')+cell(m.gone,'gone');
  el.innerHTML=h+'</tr></tbody></table>';
}

function drawKeywords(k){
  var el=document.getElementById('keywords');
  if(!k||!k.has_data){el.innerHTML='<div class="empty">暂无关注词数据</div>';return;}
  var h='<div class="meta" style="margin-bottom:8px">共 '+n(k.keywords.length)+' 个关注词 · 走势为最近 '+
    n(k.rounds.length)+' 轮命中平台数（0=该轮未上热榜）</div>';
  h+='<table><thead><tr><th>关注词</th><th>方向</th><th class="r">命中平台</th><th class="r">最佳榜位</th><th class="r">合计热度</th><th>近 '+n(k.rounds.length)+' 轮走势</th></tr></thead><tbody>';
  k.keywords.forEach(function(x){
    var mini='<span class="mini">';
    (x.trend||[]).forEach(function(t){
      var hh=Math.max(2,Math.round((t.hits||0)/4*16));
      mini+='<i class="'+(t.hits>0?'on':'')+'" style="height:'+hh+'px" title="'+esc(t.round)+' 命中 '+n(t.hits)+' 平台"></i>';
    });
    mini+='</span>';
    h+='<tr><td><b>'+esc(x.keyword)+'</b></td><td>'+esc(x.category||'—')+'</td>'+
       '<td class="r">'+n(x.hits)+'</td><td class="r">'+n(x.best_rank)+'</td>'+
       '<td class="r heat">'+n(x.total_heat)+'</td><td>'+mini+'</td></tr>';
  });
  el.innerHTML=h+'</tbody></table>';
}

function drawExports(){
  var tables=['trends','works','creators','work_snapshots','creator_snapshots',
              'content_analysis','topic_opportunities','keyword_snapshots',
              'tracked_keywords','collection_runs','source_catalog'];
  var h='';
  tables.forEach(function(t){h+='<a class="exp" href="/api/export/'+t+'.csv">'+t+'.csv</a>';});
  h+='<a class="exp" href="/api/export/all.json">全量 JSON</a>';
  if(EMBED) h+='<div class="meta" style="margin-top:8px">当前为离线快照，下载需启动看板服务：<code>python3 tools/dashboard.py</code></div>';
  document.getElementById('exports').innerHTML=h;
}

function draw(d){
  var o=d.overview||{};
  drawFreshbar(o);
  drawCards(o); drawBrief(d.brief); drawKnowledge(d.knowledge); drawSmzdm(d.smzdm);
  drawContinuity(d.continuity, d.maintenance, d.quality); drawHealth(o, d.source_stats);
  drawMovements(d.movements); drawKeywords(d.keywords);
  drawTrends(d.trends);
  drawDouyin(d.trends); drawNews(d.news);
  drawWorks(d.works); drawCoverage(d.coverage); drawCrosscheck(d.crosscheck);
  drawTopics(d.topics); drawCreators(d.creators);
  drawGrowth(d.growth); drawTimeline(d.timeline);
  drawExports(); drawFooter(o);
}

async function load(manual){
  try{
    var d = EMBED || await (await fetch('/api/all')).json();
    draw(d);
    if(manual) document.getElementById('meta').textContent += ' · 已手动刷新';
  }catch(e){
    document.getElementById('meta').textContent='加载失败: '+e;
  }
}
load(false);
if(!EMBED){ setInterval(function(){load(false);}, 15000); }
</script>
</body>
</html>
"""


def render(embed) -> str:
    raw = jdump(embed) if embed is not None else "null"
    # 防止数据中出现 </script> 提前闭合脚本块
    raw = raw.replace("</", "<\\/")
    return HTML.replace("__DATA_JSON__", raw)


def main():
    ap = argparse.ArgumentParser(description="实时可视化看板")
    ap.add_argument("--export", action="store_true", help="导出静态 HTML 快照后退出")
    ap.add_argument("--out", default=str(ROOT / "创作者情报看板.html"))
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args()
    if args.export:
        out = Path(args.out)
        out.write_text(render(all_data()), encoding="utf-8")
        print(f"已导出静态看板快照: {out} ({out.stat().st_size} bytes)")
    else:
        try:
            from waitress import serve
            print(f"看板服务启动(waitress/生产级): http://localhost:{args.port}", flush=True)
            serve(app, host=args.host, port=args.port, threads=4)
        except ImportError:
            print(f"看板服务启动(Flask 开发服务器): http://localhost:{args.port}", flush=True)
            app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
