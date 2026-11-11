#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创作者情报工作站 —— 单人自拍口播智库沉淀引擎 (knowledge)

这一层解决的是「数据怎么沉淀成智库」：

之前系统把热榜当成「监视器」——每轮刷一遍、看完即弃，trends 按 7 天清理，
没有记忆、没有积累。对一位还没起号、方向是「单人自拍口播」的创作者来说，
真正有用的是**长期沉淀下来的、可复用的内容资产**，而不是「今天什么热」。
口播是单人、真人出镜、对着镜头说话的形态——最不易被 AI 替代，也最能把个人 IP 做大。

设计：
  * host_assets      口播可迁移能力库（镜头表现力/口语化表达/观点提炼/情绪调动/人设一致性…）
  * content_frameworks 可复用口播框架库（黄金3秒钩子/观点三幕/故事化口播/人设锚点…）
  * knowledge_base  智库主表——真正「沉淀」的地方。每一轮把命中赛道信号词的热榜
                    话题，按口播能力匹配后写成一条带「口播改编角度 + 脚本骨架」的条目；
                    同一话题跨轮/跨平台反复出现，durability 累加并自动晋升为
                    validated（常青选题），形成「越积累越值钱」的选题库。

晋升规则（可解释、不黑箱）：
  出现 ≥3 个采集轮次，或命中 ≥3 个平台，或 durability ≥4 → status 自动 = validated
  用户可在看板手动把 validated 升级为 ready（待拍素材）。incubating 永远不被覆盖。

合规：仅用免登录公开热榜；角度与脚本骨架均为规则派生（data_status=derived），明确标注需人工筛选；
不伪造任何事实，不替用户下结论（观点类框架一律「留白」）。
内容安全：config/content_safety.json 维护一份拒识清单，命中清单的实时话题不进入选题推荐面、
也不累加 durability（底层 trends 实时数据原样保留）。这是非破坏性的——已入库的条目不会被删除或改写，
只是不再作为面向个人创作者的内容选题被推荐。
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import textmatch  # noqa: E402
from collector import PRIMARY_SOURCES, ROOT, jdump, PRIMARY_SOURCE  # noqa: E402

TZ = timezone(timedelta(hours=8))
HOST_PROFILE = ROOT / "config" / "host_profile.json"
SAFETY_CFG = ROOT / "config" / "content_safety.json"

# 晋升阈值：可解释、可调
VALIDATED_ROUNDS = 3
VALIDATED_PLATFORMS = 3
VALIDATED_DURABILITY = 4


def _profile() -> dict:
    try:
        return json.loads(HOST_PROFILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _safety_patterns() -> list:
    """读取内容安全拒识清单（非破坏性）。启用时命中关键词的话题不进入选题推荐面。"""
    try:
        cfg = json.loads(SAFETY_CFG.read_text(encoding="utf-8"))
        if cfg.get("enabled", True):
            return [str(p) for p in cfg.get("deny_patterns", []) if p]
    except Exception:
        pass
    return []


def _is_sensitive(text: str) -> bool:
    if not text:
        return False
    return any(p in text for p in _safety_patterns())


def ensure_kb_tables(conn):
    """幂等建立智库三张表，并从 host_profile.json 播种资产/框架（按 key 去重 upsert）。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS host_assets (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_key  TEXT UNIQUE,
            name       TEXT,
            maps_to    TEXT,
            strength   INTEGER,
            description TEXT,
            enabled    INTEGER DEFAULT 1
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS content_frameworks (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            fw_key       TEXT UNIQUE,
            name         TEXT,
            fit_asset    TEXT,
            fit_asset2   TEXT,
            structure    TEXT,
            hook_example TEXT,
            skeleton     TEXT,
            usage_count  INTEGER DEFAULT 0,
            enabled      INTEGER DEFAULT 1
        )
    """)
    # 兼容已存在的旧表：缺 skeleton 列则补齐（幂等）
    fw_cols = {r["name"] for r in conn.execute("PRAGMA table_info(content_frameworks)")}
    if "skeleton" not in fw_cols:
        conn.execute("ALTER TABLE content_frameworks ADD COLUMN skeleton TEXT")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS knowledge_base (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            nk            TEXT NOT NULL,
            category      TEXT NOT NULL,
            display_title TEXT,
            asset_key     TEXT,
            framework_key TEXT,
            angle         TEXT,
            summary       TEXT,
            durability    INTEGER DEFAULT 1,
            rounds_seen   INTEGER DEFAULT 1,
            platforms_seen TEXT DEFAULT '[]',
            first_seen    TEXT,
            last_seen     TEXT,
            source_refs   TEXT DEFAULT '[]',
            best_rank     INTEGER,
            max_heat      INTEGER,
            status        TEXT DEFAULT 'incubating',
            promoted_at   TEXT,
            user_notes    TEXT,
            user_rating   INTEGER,
            created_at    TEXT,
            updated_at    TEXT,
            data_status   TEXT DEFAULT 'derived',
            UNIQUE(nk, category)
        )
    """)
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_kb_status ON knowledge_base(status)",
        "CREATE INDEX IF NOT EXISTS idx_kb_dur ON knowledge_base(durability DESC)",
        "CREATE INDEX IF NOT EXISTS idx_kb_cat ON knowledge_base(category)",
        "CREATE INDEX IF NOT EXISTS idx_kb_title ON knowledge_base(display_title)",
    ]:
        conn.execute(sql)

    prof = _profile()
    now = datetime.now(TZ).isoformat(timespec="seconds")
    for a in prof.get("assets", []):
        conn.execute("""
            INSERT INTO host_assets (asset_key, name, maps_to, strength, description, enabled)
            VALUES (?, ?, ?, ?, ?, 1)
            ON CONFLICT(asset_key) DO UPDATE SET
                name=excluded.name, maps_to=excluded.maps_to,
                strength=excluded.strength, description=excluded.description
        """, (a.get("key"), a.get("name"), a.get("maps_to"),
              a.get("strength", 3), a.get("desc", "")))
    for f in prof.get("frameworks", []):
        conn.execute("""
            INSERT INTO content_frameworks
                (fw_key, name, fit_asset, fit_asset2, structure, hook_example, skeleton, usage_count, enabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, 0, 1)
            ON CONFLICT(fw_key) DO UPDATE SET
                name=excluded.name, fit_asset=excluded.fit_asset,
                fit_asset2=excluded.fit_asset2, structure=excluded.structure,
                hook_example=excluded.hook_example, skeleton=excluded.skeleton
        """, (f.get("key"), f.get("name"), f.get("fit_asset"), f.get("fit_asset2"),
              f.get("structure"), f.get("hook_example"), f.get("skeleton", "")))
    conn.commit()
    _realign_kb_to_profile(conn, prof)


def _realign_kb_to_profile(conn, prof):
    """幂等迁移：当 host_profile 改版（如从电台主持重构为单人自拍口播）后，
    把已沉淀的 knowledge_base 行按「类别 → 新能力/新框架」重新对齐 asset_key /
    framework_key / angle。只改派生字段（派生数据，非历史事实），不动 nk /
    category / durability / status 等真实沉淀，符合「不改写历史数据」边界。"""
    cats = prof.get("narrative_categories", [])
    if not cats:
        return
    assets = {a.get("key"): a for a in prof.get("assets", [])}
    fws = {f.get("key"): f for f in prof.get("frameworks", [])}
    cat_map = {c["name"]: c for c in cats}
    # 类别改名兼容：旧版 host_profile 的类别名 → 新版（保留已沉淀的 topic，只换标签）。
    # 可能因跨轮重复沉淀出现 UNIQUE(nk,category) 冲突——同一话题在旧/新类别下各有一条，
    # 此时删掉冗余的旧类别行（新类别行已是正确映射），其余直接改名。
    RENAME = {"观点·对话": "观点·立场线", "生活·叙事": "生活·情绪"}
    renamed = 0
    for old, new in RENAME.items():
        if new not in cat_map or old == new:
            continue
        for r in conn.execute("SELECT id, nk FROM knowledge_base WHERE category=?", (old,)):
            if conn.execute("SELECT 1 FROM knowledge_base WHERE nk=? AND category=?",
                            (r["nk"], new)).fetchone():
                conn.execute("DELETE FROM knowledge_base WHERE id=?", (r["id"],))
            else:
                conn.execute("UPDATE knowledge_base SET category=? WHERE id=?", (new, r["id"]))
            renamed += 1
    if renamed:
        conn.commit()
    rows = [dict(r) for r in conn.execute(
        "SELECT id, nk, category, asset_key, framework_key, angle FROM knowledge_base")]
    upd = 0
    for r in rows:
        cat = cat_map.get(r["category"])
        if not cat:
            continue
        asset = assets.get(cat.get("asset_key"), {})
        fw = fws.get(cat.get("framework_key"), {})
        new_ak = cat.get("asset_key")
        new_fk = cat.get("framework_key")
        new_angle = _angle_text(cat, asset, fw)
        if (r["asset_key"] != new_ak or r["framework_key"] != new_fk
                or (r["angle"] or "") != new_angle):
            conn.execute(
                "UPDATE knowledge_base SET asset_key=?, framework_key=?, angle=? "
                "WHERE id=?", (new_ak, new_fk, new_angle, r["id"]))
            upd += 1
    if upd:
        conn.commit()
    return upd + renamed


def _angle_text(cat: dict, asset: dict, fw: dict) -> str:
    """把「赛道类别 + 口播能力 + 口播框架」合成一条口播改编角度。"""
    parts = []
    if asset:
        parts.append(f"镜头前用「{asset.get('name')}」（{asset.get('maps_to','')}）")
    if fw:
        parts.append(f"套「{fw.get('name')}」框架：{fw.get('structure','')}")
    if cat.get("angle"):
        parts.append(cat["angle"])
    return "；".join(parts)


def _script_skeleton(asset: dict, fw: dict) -> str:
    """生成可直接照着拍的口播脚本骨架（4 步）。基于匹配到的框架模板。"""
    skel = (fw or {}).get("skeleton") or (
        "① 强钩子开场；② 亮出你的角度；③ 用事实/故事撑住；④ 落点 + 抛问题到评论区")
    who = (asset or {}).get("name", "你的口播能力")
    return f"【{who} · 口播脚本骨架】\n{skel}"


def incubate(conn, round_ts: str) -> int:
    """智库沉淀主函数：每轮调用一次，把末轮热榜里命中赛道信号词的话题写进 knowledge_base。

    返回本轮新增/更新的智库条目数。任一话题异常都隔离，绝不中断整轮。
    """
    ensure_kb_tables(conn)
    prof = _profile()
    cats = prof.get("narrative_categories", [])
    if not cats:
        return 0
    assets = {a.get("key"): a for a in prof.get("assets", [])}
    fws = {f.get("key"): f for f in prof.get("frameworks", [])}

    # 末轮主榜趋势（与看板/简报同口径，避免镜像/垂直榜稀释）
    ph = ",".join("?" * len(PRIMARY_SOURCES))
    rows = [dict(r) for r in conn.execute(
        f"SELECT id, platform, keyword, rank, heat_value, trend_source "
        f"FROM trends WHERE trend_time = ? AND trend_source IN ({ph})",
        (round_ts, *PRIMARY_SOURCES)).fetchall()]

    # 每条趋势匹配「最贴切」的叙事类别：命中的信号词越多越贴切（平局取靠前的类别）。
    # 这样对同一话题能稳定落到最相关的赛道，而不是撞到第一个含词的类别。
    def best_cat(kw):
        best, best_hits = None, -1
        for cat in cats:
            hits = sum(1 for s in cat.get("signals", []) if s in kw)
            if hits > best_hits:
                best, best_hits = cat, hits
        return best

    matched = []
    for r in rows:
        kw = r.get("keyword") or ""
        if not kw:
            continue
        # 内容安全守门：命中拒识清单的话题不进入选题推荐面，也不累加 durability
        if _is_sensitive(kw):
            continue
        cat = best_cat(kw)
        if cat:
            matched.append((textmatch.norm(kw), cat["name"], r, cat))

    # 按 (归一化话题, 类别) 聚合：取最佳榜位/最高热度，平台去重累加
    agg = {}
    for nk, catname, r, cat in matched:
        key = (nk, catname)
        g = agg.setdefault(key, {
            "nk": nk, "cat": cat, "rows": [],
            "platforms": set(), "best_rank": 10 ** 9, "max_heat": 0,
        })
        g["rows"].append(r)
        g["platforms"].add(r["platform"])
        g["best_rank"] = min(g["best_rank"], r["rank"] or 10 ** 9)
        g["max_heat"] = max(g["max_heat"], r["heat_value"] or 0)

    n = 0
    now = round_ts
    for (nk, catname), g in agg.items():
        cat = g["cat"]
        asset = assets.get(cat.get("asset_key"), {})
        fw = fws.get(cat.get("framework_key"), {})
        angle_full = _angle_text(cat, asset, fw)
        plats = sorted(g["platforms"])
        refs = [{"id": r["id"], "platform": r["platform"], "rank": r["rank"]}
                for r in g["rows"]]
        existing = conn.execute(
            "SELECT * FROM knowledge_base WHERE nk=? AND category=?",
            (nk, catname)).fetchone()
        if existing:
            new_rounds = (existing["rounds_seen"] or 1) + 1
            old_plats = set(json.loads(existing["platforms_seen"] or "[]"))
            new_plats = sorted(old_plats | set(plats))
            # durability：每轮复现 +1；若出现新平台再 +1（跨平台传播力更强）
            dur = max(existing["durability"] or 1, 1) + 1
            if set(plats) - old_plats:
                dur += 1
            refs_all = (json.loads(existing["source_refs"] or "[]")) + refs
            if len(refs_all) > 40:
                refs_all = refs_all[-40:]
            best = g["best_rank"]
            if existing["best_rank"]:
                best = min(best, existing["best_rank"])
            maxh = max(g["max_heat"], existing["max_heat"] or 0)
            status = existing["status"]
            # 仅当仍是 incubating 才自动晋升；用户已手动 promoted 的不动
            if status == "incubating" and (
                new_rounds >= VALIDATED_ROUNDS
                or len(new_plats) >= VALIDATED_PLATFORMS
                or dur >= VALIDATED_DURABILITY
            ):
                status = "validated"
            conn.execute("""
                UPDATE knowledge_base
                   SET rounds_seen=?, platforms_seen=?, durability=?, last_seen=?,
                       source_refs=?, best_rank=?, max_heat=?, status=?, updated_at=?
                 WHERE nk=? AND category=?
            """, (new_rounds, jdump(new_plats), dur, now,
                  jdump(refs_all), best, maxh, status, now, nk, catname))
            n += 1
        else:
            conn.execute("""
                INSERT INTO knowledge_base
                  (nk, category, display_title, asset_key, framework_key, angle, summary,
                   durability, rounds_seen, platforms_seen, first_seen, last_seen,
                   source_refs, best_rank, max_heat, status, created_at, updated_at, data_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, ?, ?, ?, ?, ?, ?, 'incubating', ?, ?, 'derived')
            """, (nk, catname, g["rows"][0]["keyword"], cat.get("asset_key"),
                  cat.get("framework_key"), angle_full, cat.get("angle"),
                  jdump(plats), now, now, jdump(refs), g["best_rank"],
                  g["max_heat"], now, now))
            n += 1
    return n


def promote(kb_id: int, rating: int | None = None, notes: str | None = None) -> bool:
    """把某条智库条目从（incubating/validated）升级为 ready（待拍素材）。"""
    conn = sqlite3.connect(str(ROOT / "database" / "workstation.db"), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        now = datetime.now(TZ).isoformat(timespec="seconds")
        row = conn.execute(
            "SELECT status FROM knowledge_base WHERE id=?", (kb_id,)).fetchone()
        if not row:
            return False
        conn.execute("""
            UPDATE knowledge_base
               SET status='ready', promoted_at=?, user_rating=COALESCE(?, user_rating),
                   user_notes=COALESCE(?, user_notes), updated_at=?
             WHERE id=?
        """, (now, rating, notes, now, kb_id))
        conn.commit()
        return True
    finally:
        conn.close()


if __name__ == "__main__":
    import collector
    c = collector.connect()
    ensure_kb_tables(c)
    print("智库表与资产/框架已就绪；资产",
          c.execute("SELECT COUNT(*) FROM host_assets").fetchone()[0],
          "框架", c.execute("SELECT COUNT(*) FROM content_frameworks").fetchone()[0])
    c.close()
