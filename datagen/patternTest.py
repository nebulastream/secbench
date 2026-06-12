#!/usr/bin/env python3
"""
patternTest_timeseries.py

TCP client that reads newline-delimited tuples, extracts the last CSV field as
an integer timestamp, counts rows per timestamp bucket, and plots the full
history with x-axis = real time (timestamp).

Usage:
  python3 patternTest_timeseries.py --host 127.0.0.1 --port 8080 --duration 5 \
      --period-sec 1 --plot out.png

  # Interactive window:
  python3 patternTest_timeseries.py --host 127.0.0.1 --port 8080 --duration 5 \
      --period-sec 1 --show
"""

import argparse
import socket
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional, Tuple


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
    ap.add_argument("--duration", type=float, default=0.0,
                    help="Run for N seconds (0 = until Ctrl+C).")
    ap.add_argument("--period-sec", type=float, default=1.0,
                    help="Draw separators every this many seconds (distribution period).")
    ap.add_argument("--plot", default="",
                    help="Save time-series plot to this file (requires matplotlib).")
    ap.add_argument("--show", action="store_true",
                    help="Show plot interactively (requires matplotlib).")
    ap.add_argument("--csv", default="",
                    help="Write full history as CSV: timestamp,count (sorted by timestamp).")
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
        return 0

    # Sort full history by timestamp
    hist = sorted(counts.items(), key=lambda kv: kv[0])  # (ts, count)
    first_ts = hist[0][0]
    unit = infer_ts_unit(first_ts)

    # Convert to datetimes for real-time x-axis
    xs_sec = [ts_to_seconds(ts, unit) for ts, _ in hist]
    xs_dt = [datetime.fromtimestamp(s, tz=timezone.utc) for s in xs_sec]
    ys = [c for _, c in hist]

    if args.csv:
        with open(args.csv, "w", encoding="utf-8") as out:
            out.write("timestamp,count\n")
            for ts, c in hist:
                out.write(f"{ts},{c}\n")
        print(f"Wrote CSV: {args.csv}")

    if not (args.plot or args.show):
        return 0

    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except Exception as e:
        print(f"matplotlib not available ({e}). Install with: pip install matplotlib", file=sys.stderr)
        return 0

    fig, ax = plt.subplots()

    ax.plot(xs_dt, ys)
    ax.set_xlabel(f"timestamp (UTC) [unit guessed: {unit}]")
    ax.set_ylabel("rows in that timestamp bucket")
    ax.set_title("Rows per timestamp bucket over time (full history)")

    # Nice time formatting on x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    fig.autofmt_xdate()

    # Draw period separators (e.g., every 1 second) starting from first bucket
    period = max(1e-6, float(args.period_sec))
    t0 = xs_sec[0]
    t_end = xs_sec[-1]
    k = 0
    while True:
        boundary = t0 + k * period
        if boundary > t_end:
            break
        ax.axvline(datetime.fromtimestamp(boundary, tz=timezone.utc), linewidth=1)
        k += 1

    fig.tight_layout()

    if args.plot:
        plt.savefig(args.plot)
        print(f"Saved plot: {args.plot}")

    if args.show:
        plt.show()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
