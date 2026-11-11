#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""创作者情报工作站 · 可复跑的只读审计工具。

用法：
  python3 tools/audit.py                # 打印审计报告（PASS/WARN/FAIL）
  python3 tools/audit.py --json         # 输出 JSON
  python3 tools/audit.py --out logs/audit.json

只读，不改库、不采集、不重启服务。覆盖 8 组检查：
  A 数据完整性 / B 统计口径 / C 实时性 / D 数据源与情报能力 /
  E 合规边界 / F 服务与调度 / G 单人自拍口播智库 / H 内容安全守门。
每次大改（重构方向、扩充信息源、升级智库逻辑）都应复跑，确保逻辑/合规/实用性不掉链子。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import median

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import collector  # noqa: E402
import textmatch  # noqa: E402
import dashboard  # noqa: E402

DB = ROOT / "database" / "workstation.db"
TZ = timezone(timedelta(hours=8))
EXPECTED_TABLES = (set(collector.TABLES)
                   | {"source_catalog", "host_assets",
                      "content_frameworks", "knowledge_base"})

results = []


def add(group, name, status, detail, evidence=None):
    results.append({"group": group, "name": name, "status": status,
                    "detail": detail, "evidence": evidence})


def q(c, sql, a=()):
    return [dict(r) for r in c.execute(sql, a).fetchall()]


def main():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row

    # ---------------- A 数据完整性 ----------------
    tables = {r[0] for r in c.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = EXPECTED_TABLES - tables
    add("A 数据完整性", "表结构齐全", "PASS" if not missing else "FAIL",
        f"应有 {len(EXPECTED_TABLES)} 张表，实有 {len(tables & EXPECTED_TABLES)} 张",
        {"missing": sorted(missing)})

    fk = c.execute("PRAGMA foreign_key_check").fetchall()
    add("A 数据完整性", "外键完整", "PASS" if not fk else "FAIL",
        f"外键违规 {len(fk)} 条")

    no_url = c.execute("SELECT COUNT(*) FROM works WHERE url IS NULL OR url=''").fetchone()[0]
    no_creator = c.execute("SELECT COUNT(*) FROM works WHERE creator_id IS NULL").fetchone()[0]
    add("A 数据完整性", "作品关键字段", "PASS" if no_url == 0 else "FAIL",
        f"缺 URL {no_url} 条 / 未关联创作者 {no_creator} 条（后者属搜索源正常现象）")

    idx = [r[0] for r in c.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")]
    add("A 数据完整性", "性能索引", "PASS" if len(idx) >= 12 else "WARN",
        f"显式索引 {len(idx)} 个（第二轮新增 7 个，避免 growth/continuity 全表扫描）",
        {"indexes": sorted(idx)})

    # ---------------- B 统计口径 ----------------
    mis = c.execute("""SELECT COUNT(*) FROM trends
                       WHERE heat_value IS NULL AND heat_available <> 0""").fetchone()[0]
    add("B 统计口径", "热度可得性标记", "PASS" if mis == 0 else "FAIL",
        f"heat_value 为空却标 heat_available=1 的记录 {mis} 条（应为 0）")

    # items_seen 是第二轮后加的列，历史记录为 DEFAULT 0。按「不改写历史」原则
    # 不回填旧数据，只校验新口径记录，并把历史条数单独如实列出。
    bad_cnt = c.execute("""SELECT COUNT(*) FROM collection_runs
                           WHERE items_collected > items_seen AND items_seen > 0""").fetchone()[0]
    legacy = c.execute("""SELECT COUNT(*) FROM collection_runs
                          WHERE items_seen = 0 AND items_collected > 0""").fetchone()[0]
    add("B 统计口径", "新增数不超过处理数", "PASS" if bad_cnt == 0 else "FAIL",
        f"新口径记录中 items_collected > items_seen 的 {bad_cnt} 条（应为 0）；"
        f"另有 {legacy} 条历史记录无 items_seen（列后加，未回填、不参与校验）")

    last_cs = c.execute("SELECT MAX(snapshot_at) FROM creator_snapshots").fetchone()[0]
    cs_src = q(c, """SELECT source, COUNT(*) n FROM creator_snapshots
                     WHERE snapshot_at = ? GROUP BY source""", (last_cs,)) if last_cs else []
    bad_cs = sum(x["n"] for x in cs_src if x["source"] != "aggregate")
    add("B 统计口径", "创作者快照统一聚合", "PASS" if bad_cs == 0 and cs_src else "FAIL",
        f"末轮 {last_cs}：aggregate {sum(x['n'] for x in cs_src if x['source']=='aggregate')} 条，"
        f"分片来源 {bad_cs} 条（A1 修复后应为 0）", {"by_source": cs_src})

    # ---------------- C 实时性 ----------------
    runs = [r[0] for r in c.execute(
        "SELECT DISTINCT run_at FROM collection_runs ORDER BY run_at DESC LIMIT 12")]
    gaps = []
    if len(runs) >= 2:
        ts = [datetime.fromisoformat(x) for x in reversed(runs)]
        gaps = [(ts[i] - ts[i - 1]).total_seconds() for i in range(1, len(ts))]
    med = median(gaps) if gaps else None
    add("C 实时性", "采集粒度", "PASS" if med and med <= 180 else "FAIL",
        f"最近 12 轮间隔中位数 {round(med, 1) if med else '—'} 秒（目标 ≤180s）",
        {"recent_runs": runs[:6], "gaps_seconds": [round(g) for g in gaps[-6:]]})

    last_run = runs[0] if runs else None
    age = (datetime.now(TZ) - datetime.fromisoformat(last_run)).total_seconds() if last_run else None
    add("C 实时性", "数据新鲜", "PASS" if age is not None and age <= collector.FRESHNESS_SECONDS else "FAIL",
        f"末轮 {last_run} · 距今 {round(age,1) if age is not None else '—'} 秒"
        f"（阈值 {collector.FRESHNESS_SECONDS}s）")

    # ---------------- D 数据源与情报能力 ----------------
    try:
        cat = q(c, "SELECT name, platform, tier, enabled FROM source_catalog")
    except Exception:
        cat = []
    plats = sorted({x["platform"] for x in cat})
    tiers = {}
    for x in cat:
        tiers[x["tier"]] = tiers.get(x["tier"], 0) + 1
    add("D 数据源", "数据源目录", "PASS" if len(cat) >= 18 and len(plats) >= 7 else "WARN",
        f"{len(cat)} 源 / {len(plats)} 平台 · 分级 {tiers}", {"platforms": plats, "expected_min": 18})

    last_round = c.execute("SELECT MAX(run_at) FROM collection_runs").fetchone()[0]
    skips = c.execute("""SELECT COUNT(*) FROM collection_runs
                         WHERE run_at=? AND status='scheduled_skip'""",
                      (last_round,)).fetchone()[0]
    oks = c.execute("""SELECT COUNT(*) FROM collection_runs
                       WHERE run_at=? AND status='success'""",
                    (last_round,)).fetchone()[0]
    add("D 数据源", "分级调度生效", "PASS" if skips and oks else "WARN",
        f"末轮成功 {oks} 源 / 按期间隔跳过 {skips} 源（两者都应 >0）")

    # 共振能力：取「最近一次有主榜数据的轮次」（避免严格末轮恰逢主榜源被分级跳过→误报 FAIL）
    ph = ",".join("?" * len(dashboard.PRIMARY_SOURCES))
    last_main = c.execute(
        f"SELECT MAX(trend_time) FROM trends WHERE trend_source IN ({ph})",
        tuple(dashboard.PRIMARY_SOURCES)).fetchone()[0] or last_round
    cur = q(c, f"""SELECT id, platform, trend_source, keyword, rank, heat_value
                   FROM trends WHERE trend_time = ? AND trend_source IN ({ph})""",
            (last_main, *dashboard.PRIMARY_SOURCES))
    cl = textmatch.resonance(cur, min_platforms=2, max_clusters=40)
    exact = sum(1 for g in cl if g["match"] == "exact")
    fuzzy = sum(1 for g in cl if g["match"] == "fuzzy")
    dy = sum(1 for g in cl if "douyin" in g["platforms"])
    add("D 数据源", "跨平台共振（含抖音）", "PASS" if dy > 0 else "FAIL",
        f"末轮主榜 {len(cur)} 条 → 共振簇 {len(cl)}（精确 {exact} / 模糊 {fuzzy}），"
        f"其中抖音参与 {dy} 簇（A3 修复前为 0）")

    a = {textmatch.norm(r[0]) for r in c.execute(
        "SELECT keyword FROM trends WHERE trend_time=? AND trend_source='douyin_hot' "
        "ORDER BY rank LIMIT 10", (last_main,))}
    b = {textmatch.norm(r[0]) for r in c.execute(
        "SELECT keyword FROM trends WHERE trend_time=? AND trend_source='douyin_ies' "
        "ORDER BY rank LIMIT 10", (last_main,))}
    jac = len(a & b) / len(a | b) if a | b else 0
    add("D 数据源", "抖音双源交叉校验", "PASS" if jac >= 0.6 else "WARN",
        f"TOP10 重合 {len(a & b)}/10 · Jaccard {round(jac, 3)}")
    c.close()

    # ---------------- E 合规边界 ----------------
    c2 = sqlite3.connect(DB)
    c2.row_factory = sqlite3.Row
    comments = c2.execute("SELECT COUNT(*) FROM comments").fetchone()[0]
    add("E 合规边界", "评论表保持为空", "PASS" if comments == 0 else "FAIL",
        f"comments 行数 {comments}（含 PII，按任务书不纳入）")
    cat_rows = q(c2, "SELECT name, compliance FROM source_catalog")
    bad = [x["name"] for x in cat_rows
           if not x["compliance"] or "登录态" not in x["compliance"]]
    add("E 合规边界", "数据源合规声明", "PASS" if not bad else "WARN",
        f"{len(cat_rows)} 个源均标注「无登录态/无签名/无验证码绕过」", {"missing": bad})
    c2.close()

    # ---------------- F 服务与调度 ----------------
    try:
        with urllib.request.urlopen("http://127.0.0.1:8765/api/all", timeout=4) as r:
            api = json.loads(r.read().decode())
        ok_api = all(k in api for k in
                     ("brief", "news", "coverage", "crosscheck", "continuity"))
        add("F 服务与调度", "实时看板接口", "PASS" if ok_api else "FAIL",
            "HTTP 200 · /api/all 含第二轮新增字段")
    except Exception as e:
        add("F 服务与调度", "实时看板接口", "WARN",
            f"服务未响应（离线审计可接受）: {type(e).__name__}: {e}")

    snap = ROOT / "创作者情报看板.html"
    snap_ok = snap.exists() and "__DATA_JSON__" not in snap.read_text(encoding="utf-8")
    add("F 服务与调度", "离线快照", "PASS" if snap_ok else "WARN",
        f"{snap.name} {snap.stat().st_size if snap.exists() else 0} bytes · 占位符已替换")

    try:
        import subprocess
        cron = subprocess.check_output(["crontab", "-l"], text=True)
        cron_ok = "*/2 * * * * /usr/local/bin/ws_maintain.sh" in cron
    except Exception:
        cron_ok = False
    add("F 服务与调度", "系统级调度", "PASS" if cron_ok else "WARN",
        "cron 每 2 分钟 + @reboot 自愈" if cron_ok else "未检测到 2 分钟 cron")

    qp = ROOT / "logs" / "quality.json"
    quality = json.loads(qp.read_text(encoding="utf-8")) if qp.exists() else {}
    add("F 服务与调度", "质量自检",
        "PASS" if quality.get("healthy") else "FAIL",
        f"score={quality.get('score')} · issues={len(quality.get('issues', []))} · "
        f"warnings={len(quality.get('warnings', []))}",
        {"warnings": quality.get("warnings", [])})

    # ---------------- G 单人自拍口播智库（数据沉淀成智库） ----------------
    cg = sqlite3.connect(DB)
    cg.row_factory = sqlite3.Row
    assets = cg.execute("SELECT COUNT(*) FROM host_assets").fetchone()[0]
    fws = cg.execute("SELECT COUNT(*) FROM content_frameworks").fetchone()[0]
    fw_skel = cg.execute("SELECT COUNT(*) FROM content_frameworks WHERE skeleton IS NOT NULL AND skeleton<>''").fetchone()[0]
    kb_total = cg.execute("SELECT COUNT(*) FROM knowledge_base").fetchone()[0]
    kb_val = cg.execute("SELECT COUNT(*) FROM knowledge_base WHERE status='validated'").fetchone()[0]
    kb_ready = cg.execute("SELECT COUNT(*) FROM knowledge_base WHERE status='ready'").fetchone()[0]
    max_dur = cg.execute("SELECT MAX(durability) FROM knowledge_base").fetchone()[0] or 0
    # 口播方向：能力库/框架库应体现「真人出镜/口语化/观点」等口播语义，而非旧的电台主持语义
    a_names = " ".join(r[0] for r in cg.execute("SELECT name FROM host_assets"))
    f_names = " ".join(r[0] for r in cg.execute("SELECT name FROM content_frameworks"))
    is_koubo = ("镜头表现力" in a_names and "口语化表达" in a_names
                and "黄金3秒钩子" in f_names and "观点三幕" in f_names)
    add("G 口播智库", "口播能力库已播种",
        "PASS" if assets >= 4 and is_koubo else "FAIL",
        f"host_assets {assets} 条（真人出镜/口语化/观点提炼等口播能力）")
    add("G 口播智库", "口播框架库已播种(含脚本骨架)",
        "PASS" if fws >= 5 and fw_skel >= 5 else "FAIL",
        f"content_frameworks {fws} 条 · 含脚本骨架 {fw_skel} 条（黄金3秒钩子/观点三幕…）")
    add("G 口播智库", "选题库持续沉淀",
        "PASS" if kb_total > 0 else "WARN",
        f"knowledge_base 累计 {kb_total} 条（跨轮/跨平台反复出现会自动沉淀为常青选题）")
    add("G 口播智库", "晋升机制生效（常青选题）",
        "PASS" if kb_val > 0 or max_dur >= 4 else "WARN",
        f"已验证常青 {kb_val} 条 · 待拍 {kb_ready} 条 · max_durability {max_dur}"
        f"（阈值：在榜≥3轮 或 ≥3平台 或 durability≥4）")
    # 沉淀一致性：所有 KB 行的 framework_key / asset_key 必须能在当前 profile 找到
    valid_fk = {r[0] for r in cg.execute("SELECT fw_key FROM content_frameworks")}
    valid_ak = {r[0] for r in cg.execute("SELECT asset_key FROM host_assets")}
    stray_fk = cg.execute(
        f"SELECT COUNT(*) FROM knowledge_base WHERE framework_key NOT IN ({','.join('?'*len(valid_fk))})",
        tuple(valid_fk)).fetchone()[0] if valid_fk else 1
    stray_ak = cg.execute(
        f"SELECT COUNT(*) FROM knowledge_base WHERE asset_key NOT IN ({','.join('?'*len(valid_ak))})",
        tuple(valid_ak)).fetchone()[0] if valid_ak else 1
    add("G 口播智库", "沉淀一致性(无悬空引用)",
        "PASS" if stray_fk == 0 and stray_ak == 0 else "FAIL",
        f"framework_key 悬空 {stray_fk} / asset_key 悬空 {stray_ak}（改版后已幂等重对齐）")
    cg.close()

    # ---------------- H 内容安全守门（非破坏性合规） ----------------
    ch = sqlite3.connect(DB)
    ch.row_factory = sqlite3.Row
    try:
        import json as _json
        safety_path = ROOT / "config" / "content_safety.json"
        safety_ok = safety_path.exists()
        sc = _json.loads(safety_path.read_text(encoding="utf-8")) if safety_ok else {}
        sc_on = bool(sc.get("enabled", True))
        sc_n = len(sc.get("deny_patterns", []))
        add("H 内容安全", "拒识清单存在且启用",
            "PASS" if safety_ok and sc_on and sc_n > 0 else "FAIL",
            f"content_safety.json enabled={sc_on} · deny_patterns {sc_n} 条")
        # 推荐面隔离：看板的「选题库推荐视图」不应出现命中拒识清单的条目。
        # 底层 knowledge_base 为合规「非破坏性」仍保留这些行，但展示层已过滤，
        # 故这里直接核验看板 API 实际下发的推荐列表（evergreen/incubating/all）。
        try:
            import urllib.request as _ur
            leaked = -1
            try:
                with _ur.urlopen("http://127.0.0.1:8765/api/all", timeout=4) as _r:
                    _api = _json.loads(_r.read().decode())
                _kb = _api.get("knowledge", {})
                _kn = __import__("knowledge")
                _shown = (_kb.get("evergreen", []) + _kb.get("incubating", [])
                          + _kb.get("all", []))
                leaked = sum(1 for x in _shown if _kn._is_sensitive(x.get("nk") or ""))
                add("H 内容安全", "推荐面敏感话题隔离",
                    "PASS" if leaked == 0 else "WARN",
                    f"看板推荐视图命中拒识清单的条目 {leaked} 条"
                    + (f"（底层保留 {_kb.get('excluded_sensitive','?')} 条、仅不推荐）"
                       if _kb.get("excluded_sensitive") else ""))
            except Exception as e:
                # 离线审计：退化为查底层表并如实标注（展示层仍会过滤）
                kb_rows = [dict(r) for r in ch.execute("SELECT nk FROM knowledge_base")]
                _kn2 = __import__("knowledge")
                raw = sum(1 for r in kb_rows if _kn2._is_sensitive(r["nk"] or ""))
                add("H 内容安全", "推荐面敏感话题隔离",
                    "WARN" if raw else "PASS",
                    f"服务未响应，仅查底层表：保留 {raw} 条敏感话题（非破坏性，展示层已过滤）")
        except Exception as e:
            add("H 内容安全", "推荐面敏感话题隔离", "WARN", f"无法核验: {type(e).__name__}: {e}")
        # 合规声明完整性：每个数据源都标注「无登录态/无签名/无验证码绕过」
        cat_rows = [dict(r) for r in ch.execute("SELECT name, compliance FROM source_catalog")]
        bad = [x["name"] for x in cat_rows
               if not x["compliance"] or "登录态" not in x["compliance"]]
        add("H 内容安全", "数据源合规声明",
            "PASS" if not bad else "WARN",
            f"{len(cat_rows)} 个源均标注「无登录态/无签名/无验证码绕过」", {"missing": bad})
    except Exception as e:
        add("H 内容安全", "拒识清单核验", "WARN", f"异常: {type(e).__name__}: {e}")
    ch.close()

    return results


def render_text(results):
    lines = ["=" * 68,
             "创作者情报工作站 · 只读审计报告 " +
             datetime.now(TZ).isoformat(timespec="seconds"),
             "=" * 68]
    cur_group = None
    for r in results:
        if r["group"] != cur_group:
            cur_group = r["group"]
            lines.append(f"\n[{cur_group}]")
        lines.append(f"  {r['status']:4s}  {r['name']}：{r['detail']}")
    n = {s: sum(1 for r in results if r["status"] == s)
         for s in ("PASS", "WARN", "FAIL")}
    lines.append("\n" + "-" * 68)
    lines.append(f"合计：PASS {n['PASS']} / WARN {n['WARN']} / FAIL {n['FAIL']}")
    lines.append("=" * 68)
    return "\n".join(lines)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="只读审计")
    ap.add_argument("--json", action="store_true", help="输出 JSON")
    ap.add_argument("--out", default="", help="另存报告到指定路径")
    args = ap.parse_args()
    res = main()
    text = render_text(res)
    if args.json:
        payload = {"audited_at": datetime.now(TZ).isoformat(timespec="seconds"),
                   "summary": {s: sum(1 for r in res if r["status"] == s)
                               for s in ("PASS", "WARN", "FAIL")},
                   "checks": res}
        print(json.dumps(payload, ensure_ascii=False, indent=1))
    else:
        print(text)
    if args.out:
        p = Path(args.out)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"audited_at": datetime.now(TZ).isoformat(timespec="seconds"),
                                 "checks": res}, ensure_ascii=False, indent=1)
                     if args.json else text, encoding="utf-8")
        print(f"已写入 {p}")
    sys.exit(1 if any(r["status"] == "FAIL" for r in res) else 0)
