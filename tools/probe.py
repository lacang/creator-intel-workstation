#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""数据源探针：只读、不落库、不落 raw。

回答一个问题——**从当前出口 IP 出发，各源还能不能采到数据**。
典型用法是在 GitHub Actions runner（境外 IP）上跑一次，用真实结果决定能否把采集
迁到 Actions，而不是靠猜。

不写 database/，不写 */raw/，不产生任何副作用。

用法：
    python3 tools/probe.py                      # 全部源
    python3 tools/probe.py --only baidu_hot,bili_square
    python3 tools/probe.py --json probe.json    # 另存结构化结果
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import urllib3                                    # noqa: E402
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import collector as C                             # noqa: E402
import sources_ext as S                           # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", help="把结构化结果写到该路径")
    ap.add_argument("--only", help="只测这些源，逗号分隔")
    ap.add_argument("--timeout", type=int, default=12)
    args = ap.parse_args()

    cfg = C.load_settings()
    http = C.Http(timeout=args.timeout)
    src_cfg = cfg.get("sources") or {}

    names = list(C.ORDER)
    if args.only:
        want = {x.strip() for x in args.only.split(",") if x.strip()}
        names = [n for n in names if n in want]

    seed: list[str] = []
    rows: list[dict] = []
    t_all = time.time()

    print(f"{'source':22} {'state':6} {'items':>5} {'sec':>7}  note")
    print("-" * 80, flush=True)

    for name in names:
        fn = C.ADAPTERS.get(name)
        if fn is None:
            print(f"{name:22} {'N/A':6} {'-':>5} {'-':>7}  适配器未注册")
            continue
        t0 = time.time()
        ok, n, note = False, 0, ""
        try:
            data = fn(http, src_cfg.get(name, {}), seed) or {}
            ok = bool(data.get("ok"))
            n = len(data.get("items") or [])
            note = str(data.get("note") or "")
            # 与 run_once 一致：快线关键词回流给后续源做 seed
            if S.SOURCE_META.get(name, {}).get("tier") == "快线":
                seed.extend([it["keyword"] for it in (data.get("items") or [])
                             if it.get("keyword")][:3])
        except Exception as e:              # 单源异常不影响其余
            note = f"{type(e).__name__}: {e}"
        cost = round(time.time() - t0, 2)
        state = "OK" if ok and n > 0 else ("EMPTY" if ok else "FAIL")
        rows.append({"source": name, "label": S.LABELS.get(name, name),
                     "state": state, "ok": ok, "items": n,
                     "seconds": cost, "note": note[:200]})
        print(f"{name:22} {state:6} {n:5d} {cost:7.2f}  {note[:52]}", flush=True)

    total = round(time.time() - t_all, 1)
    good = [r for r in rows if r["state"] == "OK"]
    empty = [r for r in rows if r["state"] == "EMPTY"]
    fail = [r for r in rows if r["state"] == "FAIL"]

    print("-" * 80)
    print(f"合计 {len(rows)} 源 · 有数据 {len(good)} · 空响应 {len(empty)} · 失败 {len(fail)}"
          f" · 总耗时 {total}s")
    if rows:
        print(f"单源平均 {round(sum(r['seconds'] for r in rows) / len(rows), 2)}s")
    if fail:
        print("失败源：" + ", ".join(r["source"] for r in fail))

    if args.json:
        Path(args.json).write_text(
            json.dumps({"total_seconds": total, "sources": rows},
                       ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"结构化结果 -> {args.json}")

    return 0 if good else 1


if __name__ == "__main__":
    sys.exit(main())
