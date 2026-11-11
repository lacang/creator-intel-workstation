#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
系统级维护任务 —— 由 cron 每 5 分钟调用一次，**独立于 agent 会话**。

设计目的：修复「后台任务随会话结束被清掉、声称在跑其实早停了」的问题。
第二轮优化后 cron 每 2 分钟触发一次，热榜实时性由 5 分钟提升到 2 分钟；
采集内部按源分级节流（见 config/settings.json）。

自愈闭环（顺序刻意调整过，见下方说明）：
  1. 距上次采集超过 STALE_SECONDS 就立即补跑一轮采集；
  2. 先算本轮数据连续性写 logs/health.json；
  3. 再跑质量自检 verify（这样它读的是**本轮**健康，不是上一轮的陈旧快照）；
  4. 导出静态看板快照（此时 quality.json 也是本轮的，离线快照与实时服务一致）；
  5. 历史明细清理 / 每日备份 / Web 服务 watchdog；
  6. 每次执行留痕，看板显示「系统仍在运行」的时间线。

crontab 只调用 /usr/local/bin/ws_maintain.sh（纯 ASCII 路径），本文件不直接进 crontab。
"""
from __future__ import annotations

import json
import os
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))

import collector  # noqa: E402
import dashboard  # noqa: E402

PY = sys.executable
LOG = ROOT / "logs"
DB = ROOT / "database" / "workstation.db"
TZ = timezone(timedelta(hours=8))
STALE_SECONDS = 90        # cron 每 2 分钟触发；>90s 没采就补一轮
GAP_THRESHOLD = collector.FRESHNESS_SECONDS   # 600s：连续 5 轮缺失才算空窗
WEB_PORT = 8765


def last_run():
    c = sqlite3.connect(DB)
    try:
        r = c.execute("SELECT MAX(run_at) FROM collection_runs").fetchone()
        return r[0] if r else None
    finally:
        c.close()


def continuity():
    c = sqlite3.connect(DB)
    runs = [r[0] for r in c.execute(
        "SELECT DISTINCT run_at FROM collection_runs ORDER BY run_at")]
    c.close()
    ts = [datetime.fromisoformat(x) for x in runs]
    gaps = []
    for i in range(1, len(ts)):
        d = (ts[i] - ts[i - 1]).total_seconds()
        if d > GAP_THRESHOLD:
            gaps.append({
                "from": runs[i - 1], "to": runs[i],
                "minutes": round(d / 60, 1),
            })
    now = datetime.now(TZ)
    since = (now - ts[-1]).total_seconds() if ts else None
    max_gap = max([(ts[i] - ts[i - 1]).total_seconds()
                   for i in range(1, len(ts))] or [0])
    # healthy 只反映「当前是否停滞」；历史空窗作为事故记录单独保留，
    # 避免一次历史中断把看板永久钉成红色、掩盖当前正常运行的事实。
    healthy = bool(ts) and since is not None and since <= GAP_THRESHOLD
    return {
        "rounds": len(runs),
        "first": runs[0] if runs else None,
        "last": runs[-1] if runs else None,
        "since_last_minutes": round(since / 60, 1) if since is not None else None,
        "max_gap_minutes": round(max_gap / 60, 1),
        "gaps": gaps,
        "historical_gaps": len(gaps),
        "stale": since is not None and since > GAP_THRESHOLD,
        "healthy": healthy,
        "checked_at": now.isoformat(timespec="seconds"),
    }


def prune_raw(days: int = 7) -> int:
    """删除 N 天前的原始明细，避免长期运行把磁盘堆满（数据库内数据不受影响）。"""
    import time
    cutoff = time.time() - days * 86400
    n = 0
    for rel in ["02_创作者档案/raw", "03_作品数据库/raw",
                "04_评论与用户需求/raw", "05_趋势研究/raw"]:
        d = ROOT / rel
        if not d.exists():
            continue
        for f in d.glob("*.json"):
            try:
                if f.stat().st_mtime < cutoff:
                    f.unlink()
                    n += 1
            except Exception:
                pass
    return n


def backup_if_needed():
    """每天做一次一致性备份（sqlite3 backup API，WAL 安全），只保留最近 7 份。"""
    day = datetime.now(TZ).strftime("%Y%m%d")
    d = ROOT / "08_数据库备份"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"workstation_{day}.db"
    if p.exists():
        return "exists"
    src = sqlite3.connect(DB)
    dst = sqlite3.connect(p)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    for f in sorted(d.glob("workstation_*.db"))[:-7]:
        try:
            f.unlink()
        except Exception:
            pass
    return f"created:{p.stat().st_size}bytes"


def ensure_web():
    """看板服务 watchdog：端口不通、或 dashboard.py 代码有更新，就重启服务。
    避免「代码已改但服务仍跑旧版」导致接口与静态快照不一致。"""
    code = ROOT / "tools" / "dashboard.py"
    code_mtime = code.stat().st_mtime
    marker = LOG / "web_code_mtime.txt"
    s = socket.socket()
    s.settimeout(1)
    alive = s.connect_ex(("127.0.0.1", WEB_PORT)) == 0
    s.close()

    reason = None
    served_mtime = None
    if alive:
        try:
            import urllib.request
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{WEB_PORT}/api/version", timeout=2) as r:
                served_mtime = float(
                    json.loads(r.read().decode()).get("code_mtime") or 0)
        except Exception:
            served_mtime = None
    if not alive:
        reason = "down"
    elif served_mtime is None:
        reason = "legacy_service"        # 旧版服务无版本接口，必须重启
    elif abs(served_mtime - code_mtime) > 1:
        reason = "code_changed"          # 服务加载的代码已过期
    elif marker.exists():
        try:
            if float(marker.read_text(encoding="utf-8").strip()) < code_mtime - 1:
                reason = "code_changed"
        except Exception:
            pass

    if reason:
        # 精确杀掉占用 8765 的服务进程。注意 ps 输出每行以 PID 开头，
        # 不能用 startswith(命令行前缀) 匹配（曾因此导致旧进程没死、新进程 Address already in use）。
        try:
            out = subprocess.check_output(["ps", "-eo", "pid=,args="]).decode()
            prefix = f"{PY} {ROOT / 'tools' / 'dashboard.py'}"
            for line in out.splitlines():
                line = line.strip()
                if prefix in line:
                    try:
                        os.kill(int(line.split()[0]), signal.SIGTERM)
                    except Exception:
                        pass
        except Exception:
            pass
        # 兜底：直接按监听端口找 PID（ss -tlnp 输出含 pid=NNN）
        try:
            out = subprocess.check_output(["ss", "-tlnp"], text=True)
            for line in out.splitlines():
                if f":{WEB_PORT} " in line:
                    m = re.search(r"pid=(\d+)", line)
                    if m:
                        try:
                            os.kill(int(m.group(1)), signal.SIGTERM)
                        except Exception:
                            pass
        except Exception:
            pass
        time.sleep(2)  # 给端口释放留足时间
        LOG.mkdir(parents=True, exist_ok=True)
        with open(LOG / "dashboard.log", "a") as f:
            subprocess.Popen(
                [PY, str(ROOT / "tools" / "dashboard.py"), "--port", str(WEB_PORT)],
                cwd=str(ROOT), stdout=f, stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        marker.write_text(str(code_mtime), encoding="utf-8")
        # 回验：重启后端口必须真的通，日志里自证，不再出现「报了 restarted 其实没起来」
        time.sleep(2)
        s2 = socket.socket()
        s2.settimeout(1)
        ok = s2.connect_ex(("127.0.0.1", WEB_PORT)) == 0
        s2.close()
        return f"restarted({reason},verify={'ok' if ok else 'FAILED'})"

    if not marker.exists():
        marker.write_text(str(code_mtime), encoding="utf-8")
    return "alive"


def prune_db_if_due():
    """每天清理一次历史时序明细（2 分钟粒度下不清理会持续膨胀）。"""
    marker = LOG / "last_db_prune.txt"
    today = datetime.now(TZ).strftime("%Y%m%d")
    try:
        if marker.exists() and marker.read_text(encoding="utf-8").strip() == today:
            return "skip(today)"
        r = collector.prune_db()
        marker.write_text(today, encoding="utf-8")
        changed = ",".join(f"{k}={v}" for k, v in r.items() if v)
        return f"deleted:{changed or 'none'}"
    except Exception as e:
        return f"ERROR:{type(e).__name__}:{e}"


def main():
    LOG.mkdir(parents=True, exist_ok=True)
    now = datetime.now(TZ).isoformat(timespec="seconds")
    actions = []

    # 每一步独立 try/except：任何单点失败都不能连带跳过「服务拉起」和「健康快照」。
    # 顺序刻意调整为「先采集 → 先算健康 → 再自检 → 再导出」，
    # 修掉原实现 verify 读上一轮 health.json、静态快照用上一轮 quality.json 的滞后。
    try:
        lr = last_run()
        stale = True
        if lr:
            try:
                stale = (datetime.now(TZ) - datetime.fromisoformat(lr)).total_seconds() > STALE_SECONDS
            except Exception:
                stale = True
        if stale:
            s = collector.run_once()
            actions.append("collect:round=%s,ran=%d,ok=%d,skip=%d" % (
                s["round_at"], s.get("ran_sources", 0), s.get("ok_sources", 0),
                s.get("skipped_sources", 0)))
        else:
            actions.append(f"collect:skip(距上次<{STALE_SECONDS}s)")
    except Exception as e:
        actions.append(f"collect:ERROR:{type(e).__name__}:{e}")

    try:
        h = continuity()
        (LOG / "health.json").write_text(
            json.dumps(h, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception as e:
        h = {"healthy": False, "rounds": None, "gaps": [],
             "note": f"健康检查失败: {type(e).__name__}: {e}"}
        actions.append(f"health:ERROR:{type(e).__name__}:{e}")

    try:
        import verify
        q = verify.main()
        actions.append(f"verify:score={q['score']},healthy={q['healthy']}")
    except Exception as e:
        actions.append(f"verify:ERROR:{type(e).__name__}:{e}")

    try:
        out = ROOT / "创作者情报看板.html"
        out.write_text(dashboard.render(dashboard.all_data()), encoding="utf-8")
        actions.append("export:%dbytes" % out.stat().st_size)
    except Exception as e:
        actions.append(f"export:ERROR:{type(e).__name__}:{e}")

    try:
        actions.append(f"prune_raw:{prune_raw(7)}files")
    except Exception as e:
        actions.append(f"prune_raw:ERROR:{type(e).__name__}:{e}")

    try:
        actions.append("prune_db:" + prune_db_if_due())
    except Exception as e:
        actions.append(f"prune_db:ERROR:{type(e).__name__}:{e}")

    try:
        actions.append("backup:" + backup_if_needed())
    except Exception as e:
        actions.append(f"backup:ERROR:{type(e).__name__}:{e}")

    try:
        actions.append("web:" + ensure_web())
    except Exception as e:
        actions.append(f"web:ERROR:{type(e).__name__}:{e}")

    # 每次执行留痕：看板据此显示「系统仍在运行」的时间线，可随时核对有没有偷停
    try:
        hp = LOG / "maintenance_history.json"
        hist = json.loads(hp.read_text(encoding="utf-8")) if hp.exists() else []
        hist.append({"at": now, "actions": actions,
                     "rounds": h.get("rounds"), "healthy": h.get("healthy")})
        hp.write_text(json.dumps(hist[-100:], ensure_ascii=False, indent=1),
                      encoding="utf-8")
    except Exception:
        pass

    msg = "%s | %s | 轮次=%d 空窗=%d 健康=%s" % (
        now, " | ".join(actions), h.get("rounds", 0),
        len(h.get("gaps", [])), h.get("healthy"))
    print(msg, flush=True)


if __name__ == "__main__":
    main()
