#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Harbor 并发排队压测客户端（纯标准库，零依赖，Python 3.8+）。

目标：批量打 Harbor 的 OpenAI 兼容接口，量化“并发排队”行为：
  - 并发数高于上游 Provider 并发上限时，请求应该排队而不是立刻失败；
  - 排队期间的证据：X-Queue-Position 头、X-Harbor-Queue-Wait-Ms 头；
  - 队列满 / 排队超时 -> 429 + Retry-After；
  - 全部请求带 X-Request-Id，便于与 Harbor 审计对上号。

用法示例：
  # 12 并发打 60 个请求（本地 mock）
  python tools/loadtest/loadtest.py --base-url http://127.0.0.1:8080/v1 \
      --key hr-loadtest-00000000 --admin-token loadtest-admin \
      --concurrency 12 --total 60 --model smoke-1

  # 持续 30 秒，流式请求，观察排队与 429
  python tools/loadtest/loadtest.py --base-url http://127.0.0.1:8080/v1 \
      --key hr-loadtest-00000000 --concurrency 20 --duration 30 --stream

  # 打远程 Zeabur 部署（留意 RPM/并发配额，会消耗真实额度）
  python tools/loadtest/loadtest.py --base-url https://harbor.example.com/v1 \
      --key $env:HARBOR_KEY --concurrency 8 --total 40 --admin-token $env:HARBOR_ADMIN_TOKEN

  # 输出 JSON 供后续分析
  ... --json result.json
"""

import argparse
import json
import math
import os
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.request

UA = "harbor-loadtest/1.0"


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------

def percentile(vals, p):
    """线性插值分位数，空列表返回 None。"""
    if not vals:
        return None
    s = sorted(vals)
    k = (len(s) - 1) * p / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return s[int(k)]
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def fmt_ms(v):
    return "-" if v is None else f"{v:.0f}ms"


def trunc(s, n=160):
    s = (s or "").replace("\n", "\\n").replace("\r", "")
    return s if len(s) <= n else s[: n - 3] + "..."


def make_opener(insecure):
    if not insecure:
        return urllib.request.build_opener()
    ctx = ssl._create_unverified_context()
    return urllib.request.build_opener(urllib.request.HTTPSHandler(context=ctx))


class Atomic:
    """加锁的统计容器。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.done = 0
        self.ok = 0
        self.by_status = {}
        self.errors = {}  # err类 -> 次数
        self.records = []
        self.queued_count = 0
        self.max_pos = 0
        self.queue_waits = []
        self.retry_afters = []
        self.latencies = []
        self.ttfbs = []
        self.done_at = {}  # 秒级时间线: int(ts) -> count, 用于 RPS

    def add(self, rec):
        with self.lock:
            self.done += 1
            self.done_at[int(rec["t1"])] = self.done_at.get(int(rec["t1"]), 0) + 1
            st = rec["status"]
            if st == 200:
                self.ok += 1
            self.by_status[st] = self.by_status.get(st, 0) + 1
            if rec["err"]:
                self.errors[rec["err"]] = self.errors.get(rec["err"], 0) + 1
            self.latencies.append(rec["latency_ms"])
            if rec["ttfb_ms"] is not None:
                self.ttfbs.append(rec["ttfb_ms"])
            if rec["queue_pos"]:
                self.queued_count += 1
                self.max_pos = max(self.max_pos, rec["queue_pos"])
            if rec["queue_wait_ms"] is not None:
                self.queue_waits.append(rec["queue_wait_ms"])
            if rec["retry_after"]:
                self.retry_afters.append(rec["retry_after"])
            self.records.append(rec)


# ---------------------------------------------------------------------------
# 单请求执行
# ---------------------------------------------------------------------------

def send_one(opener, args, idx, deadline):
    """发一个 chat completions 请求，返回记录 dict。"""

    body = {
        "model": args.model,
        "messages": [{"role": "user", "content": args.content.format(i=idx)}],
    }
    if args.stream:
        body["stream"] = True
    if args.max_tokens:
        body["max_tokens"] = args.max_tokens
    if args.extra:
        try:
            extra = json.loads(args.extra)
        except json.JSONDecodeError:
            extra = {}
        body.update(extra)

    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        args.base_url.rstrip("/") + "/chat/completions",
        data=data,
        method="POST",
        headers={
            "Authorization": "Bearer " + args.key,
            "Content-Type": "application/json",
            "User-Agent": UA,
        },
    )

    t0 = time.time()
    rec = {
        "idx": idx,
        "t0": t0,
        "t1": None,
        "latency_ms": None,
        "ttfb_ms": None,
        "status": 0,
        "req_id": None,
        "queue_wait_ms": None,
        "queue_pos": 0,
        "retry_after": None,
        "err": None,
        "body": None,
        "chunks": 0,
    }

    try:
        with opener.open(req, timeout=args.timeout) as resp:
            rec["status"] = resp.status
            rec["req_id"] = resp.headers.get("X-Request-Id")
            qw = resp.headers.get("X-Harbor-Queue-Wait-Ms")
            rec["queue_wait_ms"] = int(qw) if qw and qw.isdigit() else None
            qp = resp.headers.get("X-Queue-Position")
            rec["queue_pos"] = int(qp) if qp and qp.isdigit() else 0
            rec["retry_after"] = resp.headers.get("Retry-After")

            # TTFB：读第一个字节
            first = resp.read(1)
            rec["ttfb_ms"] = (time.time() - t0) * 1000.0
            rest = resp.read()
            payload = first + rest
            if args.stream:
                lines = [ln.decode("utf-8", "replace").strip()
                         for ln in payload.split(b"\n")]
                data_lines = [ln for ln in lines if ln.startswith("data:")]
                rec["chunks"] = len(data_lines)
                rec["body"] = data_lines[-1][6:] if data_lines else trunc(payload.decode("utf-8", "replace"))
            else:
                rec["body"] = trunc(payload.decode("utf-8", "replace"))
            rec["status"] = resp.status
    except urllib.error.HTTPError as e:
        rec["status"] = e.code
        rec["req_id"] = e.headers.get("X-Request-Id")
        qw = e.headers.get("X-Harbor-Queue-Wait-Ms")
        rec["queue_wait_ms"] = int(qw) if qw and qw.isdigit() else None
        qp = e.headers.get("X-Queue-Position")
        rec["queue_pos"] = int(qp) if qp and qp.isdigit() else 0
        rec["retry_after"] = e.headers.get("Retry-After")
        rec["ttfb_ms"] = (time.time() - t0) * 1000.0
        try:
            rec["body"] = trunc(e.read().decode("utf-8", "replace"))
        except Exception:
            rec["body"] = None
        if e.code >= 500:
            rec["err"] = f"http_{e.code}"
    except urllib.error.URLError as e:
        rec["err"] = f"url_error: {e.reason}"[:80]
        rec["body"] = None
    except (socket.timeout, TimeoutError) as e:
        rec["err"] = f"timeout>{args.timeout}s"
        rec["body"] = None
    except Exception as e:
        rec["err"] = f"{type(e).__name__}: {e}"[:80]
        rec["body"] = None

    rec["t1"] = time.time()
    rec["latency_ms"] = (rec["t1"] - rec["t0"]) * 1000.0
    return rec


def worker(args, opener, idx, jobq, state, stop):
    """单 worker：循环取任务直到总任务数/时长到达。"""
    if args.ramp and args.concurrency > 1:
        time.sleep(idx * (args.ramp / max(1, args.concurrency - 1)))
    while not stop.is_set():
        try:
            n = jobq.get_nowait()
        except Exception:
            return
        deadline = None  # 单请求超时由 opener timeout 负责
        rec = send_one(opener, args, n, deadline)
        state.add(rec)


# ---------------------------------------------------------------------------
# 管理台采样（/api/stats + /api/queue）
# ---------------------------------------------------------------------------

def admin_fetch(opener, base, path, token, timeout=5):
    req = urllib.request.Request(
        base.rstrip("/") + path,
        headers={"Authorization": "Bearer " + token, "User-Agent": UA},
    )
    with opener.open(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def admin_base_of(base_url):
    """管理端点在同一个 host 上，路径是 /api/...，从 base_url（含 /v1）推导。"""
    b = base_url.rstrip("/")
    for suffix in ("/v1", "/v1/"):
        if b.endswith(suffix):
            b = b[: -len(suffix)]
            break
    return b


def admin_sampler(args, opener, state, stop):
    """每间隔采样一次 /api/stats 和 /api/queue，记录队列深度曲线。"""
    base = admin_base_of(args.base_url)
    samples = []
    while not stop.is_set():
        t = time.time()
        row = {"t": t, "stats": None, "queue_depth": None, "queue_max_pos": None, "queue_items": []}
        try:
            st = admin_fetch(opener, base, "/api/stats", args.admin_token)
            row["stats"] = st
            q = st.get("queue", {})
            row["queue_depth"] = q.get("depth")
            row["queue_running"] = q.get("running")
        except Exception as e:
            row["stats_err"] = f"{e}"[:80]
        try:
            ql = admin_fetch(opener, base, "/api/queue", args.admin_token)
            if isinstance(ql, list):
                row["queue_items"] = ql
                row["queue_max_pos"] = max([it.get("position", 0) for it in ql] or [0])
        except Exception as e:
            row["queue_err"] = f"{e}"[:80]
        samples.append(row)
        stop.wait(args.interval)
    return samples


# ---------------------------------------------------------------------------
# 打印
# ---------------------------------------------------------------------------

def print_summary(args, state, samples, start, end, json_path):
    n = state.done
    dur = max(end - start, 1e-6)
    rps = n / dur

    line = "=" * 66
    print(line)
    print(f"目标     {args.base_url}/chat/completions  model={args.model}  stream={args.stream}")
    print(f"负载     concurrency={args.concurrency}  total={args.total or '∞'}  duration={args.duration or '∞'}s  timeout={args.timeout}s")
    print(line)
    print(f"完成     {n} 个请求, 总耗时 {dur:.1f}s, 吞吐 {rps:.2f} req/s")
    print(f"成功     {state.ok}  ({100.0 * state.ok / n if n else 0:.1f}%)")

    pairs = sorted(state.by_status.items())
    if pairs:
        print("状态分布 " + "  ".join(f"{k}:{v}" for k, v in pairs))
    if state.errors:
        print("错误     " + "  ".join(f"{k}:{v}" for k, v in sorted(state.errors.items())))

    print(line)
    print("延迟(总) min/avg/p50/p95/p99/max: "
          + " / ".join(fmt_ms(percentile(state.latencies, p)) for p in (0, 50, 50, 95, 99, 100)))
    if state.ttfbs:
        print("TTFB     min/avg/p50/p95/p99: "
              + " / ".join(fmt_ms(percentile(state.ttfbs, p)) for p in (0, 50, 50, 95, 99)))
    if state.queue_waits:
        print("排队等待 min/avg/p50/p95/p99/max: "
              + " / ".join(fmt_ms(percentile(state.queue_waits, p)) for p in (0, 50, 50, 95, 99, 100)))
        print(f"         {len(state.queue_waits)}/{n} 个请求返回了 X-Harbor-Queue-Wait-Ms")
    print(f"排队     {state.queued_count}/{n} 个请求带 X-Queue-Position(>0), 最大位置 {state.max_pos}")
    if state.retry_afters:
        ra = sorted(state.retry_afters)
        print(f"429      {len(state.retry_afters)} 个, Retry-After 值: {ra[:10]}{'...' if len(ra) > 10 else ''}")

    # 429 样本
    bad = [r for r in state.records if r["status"] not in (200,)]
    if bad:
        print(line)
        print(f"非 200 样本（前 {min(5, len(bad))} 条）:")
        for r in bad[:5]:
            print(f"  #{r['idx']} status={r['status']} req_id={r['req_id']} "
                  f"wait={fmt_ms(r['queue_wait_ms'])} pos={r['queue_pos']} "
                  f"retry_after={r['retry_after']} err={r['err']}")
            if r["body"]:
                print(f"      body: {trunc(r['body'], 120)}")

    # 管理台采样
    if samples:
        print(line)
        print("管理台采样 (/api/stats + /api/queue):")
        ok_samples = [s for s in samples if s.get("stats")]
        if ok_samples:
            depth = [s["queue_depth"] for s in ok_samples if s["queue_depth"] is not None]
            run = [s["queue_running"] for s in ok_samples if s["queue_running"] is not None]
            if depth:
                print(f"  queue.depth    start={depth[0]}  peak={max(depth)}  end={depth[-1]}  "
                      f"avg={sum(depth) / len(depth):.1f}  samples={len(depth)}")
            if run:
                print(f"  queue.running  start={run[0]}  peak={max(run)}  end={run[-1]}")
            st0 = ok_samples[0].get("stats", {}).get("today", {})
            stN = ok_samples[-1].get("stats", {}).get("today", {})
            for k in ("requests", "success", "failed", "queued"):
                if k in st0 and k in stN:
                    print(f"  today.{k:<9} {st0.get(k)} -> {stN.get(k)}")
            qmax = [s["queue_max_pos"] for s in samples if s.get("queue_max_pos") is not None]
            if qmax:
                print(f"  /api/queue 峰值位置 {max(qmax)}")
        else:
            print(f"  (采样失败: {samples[0].get('stats_err')})")
    print(line)

    if json_path:
        out = {
            "args": vars(args),
            "summary": {
                "total": n,
                "duration_s": round(dur, 3),
                "rps": round(rps, 3),
                "ok": state.ok,
                "by_status": state.by_status,
                "errors": state.errors,
                "latency_ms": {"min": percentile(state.latencies, 0),
                               "avg": (sum(state.latencies) / len(state.latencies)) if state.latencies else None,
                               "p50": percentile(state.latencies, 50),
                               "p95": percentile(state.latencies, 95),
                               "p99": percentile(state.latencies, 99),
                               "max": percentile(state.latencies, 100)},
                "queue_wait_ms": {"min": percentile(state.queue_waits, 0),
                                  "avg": (sum(state.queue_waits) / len(state.queue_waits)) if state.queue_waits else None,
                                  "p50": percentile(state.queue_waits, 50),
                                  "p95": percentile(state.queue_waits, 95),
                                  "p99": percentile(state.queue_waits, 99),
                                  "max": percentile(state.queue_waits, 100)},
                "queued": state.queued_count,
                "max_queue_pos": state.max_pos,
                "retry_after": state.retry_afters,
            },
            "admin_samples": samples,
            "requests": state.records,
        }
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=1)
        print(f"JSON 已写入 {json_path}")


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Harbor 并发排队压测客户端（零依赖，Python 3.8+）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--base-url", default=os.environ.get("HARBOR_BASE_URL", "http://127.0.0.1:8080/v1"),
                   help="Harbor 对外地址，含 /v1")
    p.add_argument("--key", default=os.environ.get("HARBOR_KEY", ""),
                   help="Harbor Key（Authorization: Bearer）")
    p.add_argument("--admin-token", default=os.environ.get("HARBOR_ADMIN_TOKEN", ""),
                   help="管理台 Bearer Token（提供则采样 /api/stats 与 /api/queue）")
    p.add_argument("--model", default="smoke-1", help="请求的模型名（需在 Harbor 路由中）")
    p.add_argument("-c", "--concurrency", type=int, default=10, help="并发 worker 数")
    p.add_argument("-n", "--total", type=int, default=0, help="总请求数（0 = 不限制，需配 --duration）")
    p.add_argument("-d", "--duration", type=float, default=0, help="持续秒数（0 = 不限，需配 --total）")
    p.add_argument("--stream", action="store_true", help="发流式请求 stream=true")
    p.add_argument("--max-tokens", type=int, default=16, help="max_tokens")
    p.add_argument("--timeout", type=float, default=150, help="单请求 socket 超时（秒）")
    p.add_argument("--ramp", type=float, default=0, help="渐变启动：worker 在 ramp 秒内依次进入")
    p.add_argument("--interval", type=float, default=2.0, help="管理台采样间隔（秒）")
    p.add_argument("--content", default="loadtest #{i}", help="用户消息内容，{i} 为请求序号")
    p.add_argument("--extra", default="", help="附加 JSON 字段（如 '{\"temperature\":0}'）")
    p.add_argument("--insecure", action="store_true", help="跳过 HTTPS 证书校验")
    p.add_argument("--quiet", action="store_true", help="不打印每秒进度")
    p.add_argument("--json", dest="json_path", default="", help="结果导出 JSON 路径")
    return p.parse_args(argv)


def fix_windows_console():
    """输出编码适配：真控制台走 WindowsConsoleIO（Unicode 原生正确，无需处理）；
    管道/捕获场景（PS7/终端模拟器按 UTF-8 解码原生输出）强制 UTF-8，
    避免 Python 默认 GBK 输出与 UTF-8 解码端互相错位成乱码。"""
    if sys.platform != "win32" or sys.stdout.isatty():
        return
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def main(argv=None):
    fix_windows_console()
    args = parse_args(argv)
    if not args.key:
        print("缺少 --key（或环境变量 HARBOR_KEY）", file=sys.stderr)
        return 2
    if not args.total and not args.duration:
        print("--total 与 --duration 至少给一个", file=sys.stderr)
        return 2
    if args.total < 0 or args.concurrency < 1:
        print("参数非法", file=sys.stderr)
        return 2

    opener = make_opener(args.insecure)
    state = Atomic()
    stop = threading.Event()
    samples = []

    # 任务队列
    jobq = __import__("queue").Queue()
    if args.total:
        for i in range(args.total):
            jobq.put(i)

    workers = []
    for i in range(args.concurrency):
        t = threading.Thread(target=worker, args=(args, opener, i, jobq, state, stop), daemon=True)
        t.start()
        workers.append(t)

    # 进度打印线程
    last_done, last_t = 0, time.time()
    def progress():
        nonlocal last_done, last_t
        while not stop.is_set():
            stop.wait(1.0)
            if args.quiet:
                continue
            now = time.time()
            d = state.done
            rps_win = (d - last_done) / max(now - last_t, 1e-6)
            last_done, last_t = d, now
            print(f"\r[{now - start:.0f}s] done={d}/{args.total or '∞'} "
                  f"ok={state.ok} 429={state.by_status.get(429, 0)} "
                  f"err={sum(state.errors.values())} rps={rps_win:.1f} "
                  f"pos_max={state.max_pos}", end="", flush=True)

    start = time.time()

    # 管理台采样线程
    adm_thread = None
    if args.admin_token:
        samples = []
        adm_thread = threading.Thread(
            target=lambda: samples.extend(admin_sampler(args, opener, state, stop)),
            daemon=True)
        adm_thread.start()

    pt = threading.Thread(target=progress, daemon=True)
    pt.start()

    # 等待结束条件
    try:
        if args.duration:
            stop.wait(args.duration)
            stop.set()
        else:
            while any(t.is_alive() for t in workers):
                if args.total and state.done >= args.total:
                    stop.set()
                    break
                time.sleep(0.1)
            stop.set()
        for t in workers:
            t.join(timeout=2)
    except KeyboardInterrupt:
        stop.set()
        print("\n[interrupted]", file=sys.stderr)

    if adm_thread:
        adm_thread.join(timeout=3)
    pt.join(timeout=3)

    end = time.time()
    if not args.quiet:
        print()
    print_summary(args, state, samples, start, end, args.json_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
