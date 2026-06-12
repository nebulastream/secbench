#!/usr/bin/env python3
"""
patternTest_timeseries.py

Simple TCP client:
- reads newline-delimited rows
- parses the LAST comma-separated field as an integer timestamp
- counts rows per timestamp value
- saves a simple time-series plot as a PNG (no interactive display)

Example:
  python3 patternTest_timeseries.py --host 127.0.0.1 --port 8080 --duration 10 --out timeseries.png
"""

import argparse
import socket
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

# Force non-interactive backend (PNG only)
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def parse_ts_from_line(line: str) -> Optional[int]:
    line = line.strip()
    if not line:
        return None
    last = line.rsplit(",", 1)[-1].strip()
    try:
        return int(last)
    except ValueError:
        return None


def infer_ts_unit(ts: int) -> str:
    """Heuristic based on digit count: s~10, ms~13, us~16, ns~19."""
    d = len(str(abs(ts)))
    if d >= 19:
        return "ns"
    if d >= 16:
        return "us"
    if d >= 13:
        return "ms"
    return "s"


def ts_to_seconds(ts: int, unit: str) -> float:
    if unit == "ns":
        return ts / 1e9
    if unit == "us":
        return ts / 1e6
    if unit == "ms":
        return ts / 1e3
    return float(ts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--duration", type=float, default=0.0, help="Seconds to run (0 = until server closes / Ctrl+C).")
    ap.add_argument("--out", default="timeseries.png", help="Output PNG filename.")
    ap.add_argument("--csv", default="", help="Optional CSV output: timestamp,count (sorted).")
    ap.add_argument("--show-bad-lines", action="store_true")
    args = ap.parse_args()

    counts = defaultdict(int)
    bad_lines = 0
    total_lines = 0

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(args.timeout)

    try:
        sock.connect((args.host, args.port))
    except OSError as e:
        print(f"Failed to connect to {args.host}:{args.port}: {e}", file=sys.stderr)
        return 1

    f = sock.makefile("r", encoding="utf-8", newline="")
    print(f"Connected to {args.host}:{args.port}")

    start = time.time()
    try:
        while True:
            if args.duration > 0 and (time.time() - start) >= args.duration:
                break

            line = f.readline()
            if line == "":
                print("Server closed the connection.")
                break

            total_lines += 1
            ts = parse_ts_from_line(line)
            if ts is None:
                bad_lines += 1
                if args.show_bad_lines:
                    print(f"[BAD] {line.rstrip()}")
                continue

            counts[ts] += 1

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        try:
            f.close()
        except Exception:
            pass
        try:
            sock.close()
        except Exception:
            pass

    print(f"Done. buckets={len(counts)} total_lines={total_lines} bad_lines={bad_lines}")

    if not counts:
        print("No valid timestamps parsed; no plot generated.")
        return 0

    # Sort and (optionally) write CSV
    hist = sorted(counts.items(), key=lambda kv: kv[0])  # (ts, count)
    unit = infer_ts_unit(hist[0][0])

    if args.csv:
        with open(args.csv, "w", encoding="utf-8") as out:
            out.write("timestamp,count\n")
            for ts, c in hist:
                out.write(f"{ts},{c}\n")
        print(f"Wrote CSV: {args.csv}")

    xs_sec = [ts_to_seconds(ts, unit) for ts, _ in hist]
    ys = [c for _, c in hist]

    # Decide x-axis: datetime if it looks like epoch time, else relative seconds
    # Epoch seconds ~ 1.7e9 nowadays. Anything "too small" is probably synthetic/relative.
    use_datetime = (min(xs_sec) >= 946684800.0) and (max(xs_sec) <= 4102444800.0)  # [2000..2100] window

    fig, ax = plt.subplots(figsize=(10, 4))

    if use_datetime:
        import matplotlib.dates as mdates
        xs_dt = [datetime.fromtimestamp(s, tz=timezone.utc) for s in xs_sec]
        ax.plot(xs_dt, ys)
        ax.set_xlabel(f"timestamp (UTC) [guessed unit: {unit}]")
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        fig.autofmt_xdate()
    else:
        x0 = xs_sec[0]
        xs_rel = [s - x0 for s in xs_sec]
        ax.plot(xs_rel, ys)
        ax.set_xlabel(f"time since first timestamp (s) [guessed unit: {unit}]")

    ax.set_ylabel("rows per timestamp value")
    ax.set_title("Rows per timestamp bucket (simple time series)")
    fig.tight_layout()

    fig.savefig(args.out, dpi=150, bbox_inches="tight")
    print(f"Saved plot: {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
