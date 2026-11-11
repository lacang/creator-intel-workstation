#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
创作者情报工作站 —— 准实时常驻调度器（**仅手动验证用**）

注意：生产环境的调度是系统级 cron（每 2 分钟调 ws_maintain.sh → maintain.py），
本文件不参与生产运行。历史教训：用后台任务跑调度器会随 agent 会话结束被清掉，
曾造成 329.6 分钟空窗（见 00_项目管理/第一阶段任务追踪.md 第八、九节）。
需要连续验证采集时序时才手动启动本文件。

每 interval_seconds 跑一轮采集入库，使数据库持续更新。
单一数据源失败由 collector 内部隔离，不会中断调度循环。

用法：
  python3 tools/scheduler.py                      # 按 config/settings.json 的间隔无限运行
  python3 tools/scheduler.py --interval 600      # 覆盖间隔
  python3 tools/scheduler.py --max-rounds 5      # 只跑 5 轮后退出（用于验证）

停止：kill <PID> 或发送 SIGINT/SIGTERM（会在当前轮结束后优雅退出）。
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from collector import load_settings, run_once, log  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="准实时采集调度器")
    ap.add_argument("--interval", type=int, default=0,
                    help="轮间隔秒数（0=用配置文件 interval_seconds）")
    ap.add_argument("--max-rounds", type=int, default=0,
                    help="最多跑多少轮（0=无限）")
    args = ap.parse_args()

    cfg = load_settings()
    gap = args.interval if args.interval > 0 else int(cfg.get("interval_seconds", 120))
    state = {"stop": False, "rounds": 0}

    def on_signal(signum, frame):
        log.info(f"收到信号 {signum}，将在当前轮结束后优雅退出")
        state["stop"] = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    log.info(f"调度器启动：每 {gap}s 一轮，max_rounds={args.max_rounds or '∞'}")
    while not state["stop"]:
        state["rounds"] += 1
        try:
            run_once(cfg)
        except Exception as e:
            log.exception(f"第 {state['rounds']} 轮异常（已隔离，继续下一轮）: {e}")
        if args.max_rounds and state["rounds"] >= args.max_rounds:
            break
        # 可中断的等待（每秒检查一次停止信号）
        for _ in range(max(gap, 1)):
            if state["stop"]:
                break
            time.sleep(1)
    log.info(f"调度器退出，共完成 {state['rounds']} 轮")


if __name__ == "__main__":
    main()
