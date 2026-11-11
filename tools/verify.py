#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据质量自检 —— 由 tools/maintain.py 每次维护时调用，输出 logs/quality.json。

第二轮优化后的检查重点（对应审计发现）：
  * 热度缺失要区分「真实为 0」与「平台不公开」（heat_available 标记）；
  * 源成功率排除 scheduled_skip（分级调度的正常跳过不该算失败）；
  * 健康状态用 DB 末轮时间**实时**算，不信任可能冻结的 health.json；
  * 校验创作者快照是否已切换为统一聚合口径（审计 A1）；
  * 记录数据源目录完整性与分级跳过数量，让「14 源都在」可核查。
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from collector import FRESHNESS_SECONDS  # noqa: E402  单一真源：600s

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "database" / "workstation.db"
LOG = ROOT / "logs"
TZ = timezone(timedelta(hours=8))

TABLES = ["creators", "creator_snapshots", "works", "work_snapshots",
          "comments", "content_analysis", "trends", "topic_opportunities",
          "collection_runs", "tracked_keywords", "keyword_snapshots"]


def main():
    LOG.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    issues = []          # 错误（影响可信度）
    warnings = []        # 提醒（不影响可用性）
    checks = {}

    counts = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in TABLES}
    checks["tables"] = counts

    # 1) 外键完整性
    fk = c.execute("PRAGMA foreign_key_check").fetchall()
    checks["foreign_key_violations"] = len(fk)
    if fk:
        issues.append(f"外键异常 {len(fk)} 条")

    # 2) 作品关键字段
    no_url = c.execute("SELECT COUNT(*) FROM works WHERE url IS NULL OR url=''").fetchone()[0]
    no_creator = c.execute("SELECT COUNT(*) FROM works WHERE creator_id IS NULL").fetchone()[0]
    checks["works_without_url"] = no_url
    checks["works_without_creator"] = no_creator
    if no_url:
        issues.append(f"{no_url} 条作品缺少 URL")
    if no_creator:
        warnings.append(f"{no_creator} 条作品未关联创作者（搜索源常见）")

    # 3) 快照指标空值
    null_play = c.execute(
        "SELECT COUNT(*) FROM work_snapshots WHERE play_count IS NULL").fetchone()[0]
    total_snap = counts["work_snapshots"]
    checks["snapshots_null_play"] = null_play
    if total_snap and null_play / total_snap > 0.2:
        issues.append(f"作品快照中 {null_play}/{total_snap} 条缺播放量")
    elif null_play:
        warnings.append(f"{null_play} 条快照缺播放量")

    # 4) 热度缺失：只检查「平台声称提供热度」的记录
    zero_heat = c.execute(
        "SELECT COUNT(*) FROM trends WHERE (heat_value IS NULL OR heat_value=0) "
        "AND heat_available = 1").fetchone()[0]
    by_design = c.execute(
        "SELECT COUNT(*) FROM trends WHERE heat_available = 0").fetchone()[0]
    checks["trends_zero_heat"] = zero_heat
    checks["trends_no_heat_by_design"] = by_design
    if zero_heat:
        warnings.append(f"{zero_heat} 条热点热度为 0/空（源本应提供，需留意）")
    if by_design:
        checks["note_no_heat"] = "网易新闻/知乎热搜等平台不公开热度，属设计口径，非缺陷"

    # 5) 时序轮次
    rounds = c.execute(
        "SELECT COUNT(DISTINCT run_at) FROM collection_runs").fetchone()[0]
    snap_rounds = c.execute(
        "SELECT COUNT(DISTINCT snapshot_at) FROM work_snapshots").fetchone()[0]
    checks["rounds"] = rounds
    checks["snapshot_rounds"] = snap_rounds
    if rounds == 0:
        issues.append("尚无采集轮次")
    elif rounds < 2:
        warnings.append("仅 1 轮数据，暂无跨轮趋势")

    # 6) 源成功率（排除分级调度的 scheduled_skip）
    src = [dict(r) for r in c.execute(
        "SELECT source, COUNT(*) n, "
        "SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) ok "
        "FROM collection_runs WHERE status <> 'scheduled_skip' "
        "GROUP BY source ORDER BY source")]
    checks["sources"] = src
    for s in src:
        rate = (s["ok"] or 0) / s["n"] if s["n"] else 0
        if rate < 0.8:
            warnings.append(f"{s['source']} 历史成功率 {round(rate * 100)}%（{s['ok']}/{s['n']}）")

    # 7) 数据源目录与分级调度
    try:
        checks["sources_registered"] = c.execute(
            "SELECT COUNT(*) FROM source_catalog").fetchone()[0]
        checks["sources_enabled"] = c.execute(
            "SELECT COUNT(*) FROM source_catalog WHERE enabled=1").fetchone()[0]
    except Exception:
        checks["sources_registered"] = None
    last_round = c.execute("SELECT MAX(run_at) FROM collection_runs").fetchone()[0]
    skips = c.execute(
        "SELECT COUNT(*) FROM collection_runs WHERE run_at=? "
        "AND status='scheduled_skip'", (last_round,)).fetchone()[0] if last_round else 0
    checks["scheduled_skips_last_round"] = skips

    # 8) 创作者快照口径（审计 A1 修复验证）
    try:
        cs = [dict(r) for r in c.execute(
            "SELECT source, COUNT(*) n FROM creator_snapshots "
            "WHERE snapshot_at = (SELECT MAX(snapshot_at) FROM creator_snapshots) "
            "GROUP BY source")]
        checks["creator_snapshot_sources_last_round"] = cs
        if any(r["source"] != "aggregate" for r in cs):
            warnings.append("末轮创作者快照仍含分片来源（旧实现历史数据，未改写既有事实）")
    except Exception as e:
        checks["creator_snapshot_sources_last_round"] = f"ERR:{e}"

    # 9) 实时健康：直接据 DB 末轮算滞后，不依赖可能冻结的 health.json
    live_since = None
    if last_round:
        try:
            live_since = (datetime.now(TZ) - datetime.fromisoformat(last_round)).total_seconds()
        except Exception:
            live_since = None
    checks["live_since_seconds"] = round(live_since, 1) if live_since is not None else None
    checks["freshness_threshold_seconds"] = FRESHNESS_SECONDS
    if live_since is None or live_since > FRESHNESS_SECONDS:
        issues.append("当前采集停滞（实时核对：距末轮超过阈值或无可解析时间）")

    # 10) 历史空窗（仅作事故记录，不影响当前是否健康）
    hp = LOG / "health.json"
    if hp.exists():
        try:
            h = json.loads(hp.read_text(encoding="utf-8"))
            checks["historical_gaps"] = h.get("historical_gaps", len(h.get("gaps", [])))
            if h.get("historical_gaps"):
                warnings.append(f"存在 {h['historical_gaps']} 段历史空窗（已记录，不可补造）")
        except Exception:
            pass

    # 11) 单人自拍口播智库沉淀：18 源目录/资产·框架已播种(含脚本骨架)/选题库在增长/晋升生效/一致性
    try:
        checks["host_assets"] = c.execute("SELECT COUNT(*) FROM host_assets").fetchone()[0]
        checks["content_frameworks"] = c.execute(
            "SELECT COUNT(*) FROM content_frameworks").fetchone()[0]
        checks["content_frameworks_with_skeleton"] = c.execute(
            "SELECT COUNT(*) FROM content_frameworks WHERE skeleton IS NOT NULL AND skeleton<>''").fetchone()[0]
        kb_total = c.execute("SELECT COUNT(*) FROM knowledge_base").fetchone()[0]
        checks["knowledge_base_total"] = kb_total
        checks["kb_validated"] = c.execute(
            "SELECT COUNT(*) FROM knowledge_base WHERE status='validated'").fetchone()[0]
        checks["kb_ready"] = c.execute(
            "SELECT COUNT(*) FROM knowledge_base WHERE status='ready'").fetchone()[0]
        checks["kb_incubating"] = c.execute(
            "SELECT COUNT(*) FROM knowledge_base WHERE status='incubating'").fetchone()[0]
        max_dur = c.execute(
            "SELECT MAX(durability) FROM knowledge_base").fetchone()[0] or 0
        checks["kb_max_durability"] = max_dur
        checks["sources_registered_new"] = c.execute(
            "SELECT COUNT(*) FROM source_catalog").fetchone()[0]
        # 沉淀一致性：KB 行的 framework_key / asset_key 必须能在当前 profile 找到
        valid_fk = {r[0] for r in c.execute("SELECT fw_key FROM content_frameworks")}
        valid_ak = {r[0] for r in c.execute("SELECT asset_key FROM host_assets")}
        stray = 0
        if valid_fk and valid_ak:
            stray = c.execute(
                f"SELECT COUNT(*) FROM knowledge_base WHERE framework_key NOT IN ({','.join('?'*len(valid_fk))}) "
                f"OR asset_key NOT IN ({','.join('?'*len(valid_ak))})",
                tuple(valid_fk) + tuple(valid_ak)).fetchone()[0]
        checks["kb_stray_refs"] = stray
        if checks["host_assets"] < 4:
            warnings.append("口播能力库为空/偏少（host_profile.json 未正确播种）")
        if checks["content_frameworks"] < 5 or checks["content_frameworks_with_skeleton"] < 5:
            warnings.append("口播框架库/脚本骨架为空/偏少（host_profile.json 未正确播种）")
        if checks["sources_registered_new"] < 18:
            warnings.append(f"数据源目录仅 {checks['sources_registered_new']} 个（扩充后应有 ≥18）")
        if stray > 0:
            issues.append(f"智库存在 {stray} 条悬空 framework/asset 引用（改版未重对齐）")
        # 系统跑起来后，KB 应当开始沉淀；晋升机制是否生效用 max_durability≥阈值佐证
        if kb_total == 0:
            warnings.append("智库尚未沉淀（系统刚启动或赛道信号词未命中）")
        elif max_dur < 4:
            warnings.append("智库仍在孵化（尚无话题达到常青晋升阈值，属正常积累期）")
    except Exception as e:
        checks["knowledge_base_error"] = str(e)
        warnings.append(f"智库自检失败: {e}")

    c.close()

    score = max(0, 100 - len(issues) * 15 - len(warnings) * 5)
    out = {
        "checked_at": datetime.now(TZ).isoformat(timespec="seconds"),
        "score": score,
        "healthy": len(issues) == 0,
        "issues": issues,
        "warnings": warnings,
        "checks": checks,
    }
    (LOG / "quality.json").write_text(
        json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"[verify] score={score} healthy={out['healthy']} "
          f"errors={len(issues)} warnings={len(warnings)}", flush=True)
    return out


if __name__ == "__main__":
    main()
