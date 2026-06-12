#!/usr/bin/env python3
"""
Periodically report CPU, memory, and network usage for running Docker containers.

Examples:
  python docker_metrics.py                 # sample every 2s, print a table
  python docker_metrics.py -i 5 -n 12      # sample every 5s for 12 iterations
  python docker_metrics.py --json          # output JSON lines (ndjson)
  python docker_metrics.py --network web   # only containers on the "web" network
  python docker_metrics.py --all           # include paused containers too
"""

import argparse
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
from typing import Dict, List, Optional, Tuple



def human_bytes(n: Optional[float]) -> str:
    """Format bytes to human-friendly string."""
    if n is None:
        return "-"
    try:
        n = float(n)
    except Exception:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    while n >= 1024 and i < len(units) - 1:
        n /= 1024.0
        i += 1
    if n >= 100:
        return f"{n:,.0f} {units[i]}"
    elif n >= 10:
        return f"{n:,.1f} {units[i]}"
    else:
        return f"{n:,.2f} {units[i]}"


def human_pct(p: Optional[float]) -> str:
    if p is None:
        return "-"
    try:
        return f"{p:.1f}%"
    except Exception:
        return "-"


def now_str() -> str:
    return dt.datetime.now().strftime("%H:%M:%S")


def parse_size_to_bytes(text: str) -> Optional[float]:
    """Parse Docker CLI size strings like '7.039MiB', '27.8MB', '132kB'."""
    if not text:
        return None
    text = text.strip()
    if text in {"-", "0", "0B"}:
        return 0.0

    units = {
        "b": 1,
        "kb": 1000,
        "mb": 1000 ** 2,
        "gb": 1000 ** 3,
        "tb": 1000 ** 4,
        "kib": 1024,
        "mib": 1024 ** 2,
        "gib": 1024 ** 3,
        "tib": 1024 ** 4,
    }

    i = 0
    while i < len(text) and (text[i].isdigit() or text[i] == "."):
        i += 1

    if i == 0:
        return None

    value = float(text[:i])
    unit = text[i:].strip().lower()
    multiplier = units.get(unit)
    if multiplier is None:
        return None
    return value * multiplier


def parse_percent(text: str) -> Optional[float]:
    if not text:
        return None
    text = text.strip()
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        return None


def count_cpuset_cpus(cpuset: str) -> Optional[float]:
    """Count CPUs from a cpuset string like '0-3,6,8-9'."""
    if not cpuset:
        return None

    total = 0
    for part in cpuset.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start_str, end_str = part.split("-", 1)
            start = int(start_str)
            end = int(end_str)
            total += max(0, end - start + 1)
        else:
            total += 1
    return float(total) if total > 0 else None


def effective_cpu_capacity(inspect_record: dict) -> float:
    """
    Return the CPU capacity available to a container in units of host CPUs.
    Falls back to host logical CPU count when no container-specific limit exists.
    """
    host_config = inspect_record.get("HostConfig", {}) or {}

    nano_cpus = host_config.get("NanoCpus") or 0
    if nano_cpus:
        return max(1.0, float(nano_cpus) / 1_000_000_000.0)

    cpu_quota = host_config.get("CpuQuota") or 0
    cpu_period = host_config.get("CpuPeriod") or 0
    if cpu_quota > 0 and cpu_period > 0:
        return max(1.0, float(cpu_quota) / float(cpu_period))

    cpuset_count = count_cpuset_cpus(host_config.get("CpusetCpus") or "")
    if cpuset_count:
        return cpuset_count

    cpu_count = host_config.get("CpuCount") or 0
    if cpu_count:
        return float(cpu_count)

    return float(os.cpu_count() or 1)


def normalize_cpu_percent(raw_cpu_pct: Optional[float], cpu_capacity: float) -> Optional[float]:
    """
    Normalize Docker's multi-core CPU % into a capped 0-100 range relative to
    the CPUs available to the container.
    """
    if raw_cpu_pct is None:
        return None
    if cpu_capacity <= 0:
        return None
    return min(100.0, max(0.0, float(raw_cpu_pct) / float(cpu_capacity)))


def run_docker_command(args: List[str]) -> List[dict]:
    cmd = ["docker", *args]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    lines = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        lines.append(json.loads(line))
    return lines


def get_container_list(include_all: bool, network: Optional[str]) -> List[dict]:
    # By default, list running containers; include_all adds paused, etc.
    args = ["ps"]
    if include_all:
        args.append("-a")
    args.extend(["--format", "{{json .}}"])
    containers = run_docker_command(args)
    if network:
        filtered = []
        for c in containers:
            try:
                nets = [n.strip() for n in (c.get("Networks") or "").split(",") if n.strip()]
                if network in nets:
                    filtered.append(c)
            except Exception:
                pass
        return filtered
    return containers


def get_stats(container_ids: List[str]) -> Dict[str, dict]:
    if not container_ids:
        return {}

    records = run_docker_command(["stats", "--no-stream", "--format", "{{json .}}", *container_ids])
    stats = {}
    for record in records:
        container_id = record.get("ID")
        if not container_id:
            continue

        mem_usage = record.get("MemUsage") or ""
        net_io = record.get("NetIO") or ""
        mem_parts = [p.strip() for p in mem_usage.split("/", 1)]
        net_parts = [p.strip() for p in net_io.split("/", 1)]

        stats[container_id] = {
            "cpu_pct": parse_percent(record.get("CPUPerc")),
            "mem_used": parse_size_to_bytes(mem_parts[0]) if mem_parts else None,
            "mem_limit": parse_size_to_bytes(mem_parts[1]) if len(mem_parts) > 1 else None,
            "mem_pct": parse_percent(record.get("MemPerc")),
            "rx_total": parse_size_to_bytes(net_parts[0]) if net_parts else 0.0,
            "tx_total": parse_size_to_bytes(net_parts[1]) if len(net_parts) > 1 else 0.0,
        }
    return stats


def get_container_details(container_ids: List[str]) -> Dict[str, dict]:
    if not container_ids:
        return {}

    result = subprocess.run(
        ["docker", "inspect", "--size", *container_ids],
        capture_output=True,
        text=True,
        check=True,
    )
    records = json.loads(result.stdout)

    details = {}
    for record in records:
        container_id = record.get("Id", "")[:12]
        if not container_id:
            continue
        storage_used = record.get("SizeRw")
        storage_total = record.get("SizeRootFs")
        storage_pct = None
        if storage_used is not None and storage_total:
            storage_pct = min(100.0, max(0.0, float(storage_used) / float(storage_total) * 100.0))

        details[container_id] = {
            "cpu_capacity": effective_cpu_capacity(record),
            "storage_used": storage_used,
            "storage_total": storage_total,
            "storage_pct": storage_pct,
        }
    return details


def cpu_percent(stats: dict, fallback_prev: Optional[Tuple[int, int, int]] = None) -> Optional[float]:
    """
    Calculate CPU % using Docker's formula:
      CPU% = (cpu_delta / system_delta) * online_cpus * 100
    Uses stats['precpu_stats'] when available; otherwise falls back to previous sample tuple.
    """
    try:
        cpu_stats = stats.get("cpu_stats", {})
        precpu_stats = stats.get("precpu_stats", {}) or {}
        online_cpus = (
            cpu_stats.get("online_cpus")
            or (len(cpu_stats.get("cpu_usage", {}).get("percpu_usage", []) or []) or 1)
        )

        total_now = cpu_stats.get("cpu_usage", {}).get("total_usage")
        system_now = cpu_stats.get("system_cpu_usage")

        if precpu_stats and precpu_stats.get("system_cpu_usage") and precpu_stats.get("cpu_usage", {}).get("total_usage"):
            total_prev = precpu_stats["cpu_usage"]["total_usage"]
            system_prev = precpu_stats["system_cpu_usage"]
        elif fallback_prev:
            total_prev, system_prev, online_prev = fallback_prev
            if not online_cpus:
                online_cpus = online_prev
        else:
            return None  # first sample may lack a previous reference

        cpu_delta = float(total_now) - float(total_prev)
        system_delta = float(system_now) - float(system_prev)
        if cpu_delta <= 0 or system_delta <= 0:
            return 0.0
        return (cpu_delta / system_delta) * float(online_cpus) * 100.0
    except Exception:
        return None


def mem_usage(stats: dict) -> Tuple[Optional[int], Optional[int], Optional[float]]:
    """
    Return (used_bytes, limit_bytes, percent).
    Uses stats['memory_stats']['usage'] (which already excludes cache in modern Docker).
    """
    try:
        m = stats.get("memory_stats", {}) or {}
        used = m.get("usage")
        limit = m.get("limit") or 0
        pct = (float(used) / float(limit) * 100.0) if used is not None and limit else None
        return used, limit, pct
    except Exception:
        return None, None, None


def net_bytes(stats: dict) -> Tuple[int, int]:
    """Return (rx_total_bytes, tx_total_bytes) summed across all interfaces."""
    nets = stats.get("networks", {}) or {}
    rx = 0
    tx = 0
    for data in nets.values():
        rx += int(data.get("rx_bytes", 0) or 0)
        tx += int(data.get("tx_bytes", 0) or 0)
    return rx, tx


def print_table_header():
    cols = [
        "TIME",
        "CONTAINER",
        "CPU",
        "MEM",
        "MEM%",
        "STOR",
        "STOR%",
        "RX/s",
        "TX/s",
        "RX (total)",
        "TX (total)",
    ]
    print("{:<8} {:<25} {:>7} {:>12} {:>7} {:>12} {:>7} {:>12} {:>12} {:>14} {:>14}".format(*cols))


def print_table_row(sample):
    print(
        "{time:<8} {name:<25} {cpu:>7} {mem_used:>12} {mem_pct:>7} {storage_used:>12} {storage_pct:>7} {rx_rate:>12} {tx_rate:>12} {rx_total:>14} {tx_total:>14}"
        .format(
            time=sample["time"],
            name=sample["name"][:25],
            cpu=human_pct(sample["cpu_pct"]) if sample["cpu_pct"] is not None else "-",
            mem_used=human_bytes(sample["mem_used"]),
            mem_pct=human_pct(sample["mem_pct"]),
            storage_used=human_bytes(sample["storage_used"]),
            storage_pct=human_pct(sample["storage_pct"]),
            rx_rate=(human_bytes(sample["rx_rate"]) + "/s") if sample["rx_rate"] is not None else "-",
            tx_rate=(human_bytes(sample["tx_rate"]) + "/s") if sample["tx_rate"] is not None else "-",
            rx_total=human_bytes(sample["rx_total"]),
            tx_total=human_bytes(sample["tx_total"]),
        )
    )


def main():
    parser = argparse.ArgumentParser(description="Docker container metrics (CPU, memory, network).")
    parser.add_argument("-i", "--interval", type=float, default=2.0, help="Sampling interval in seconds (default: 2.0)")
    parser.add_argument("-n", "--iterations", type=int, default=0, help="Number of iterations (0=run until Ctrl+C)")
    parser.add_argument("--json", action="store_true", help="Output as newline-delimited JSON (ndjson)")
    parser.add_argument("--all", action="store_true", help="Include non-running (paused) containers")
    parser.add_argument("--network", type=str, default=None, help="Only include containers attached to this Docker network")
    args = parser.parse_args()

    stop = {"flag": False}

    def handle_sigint(signum, frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    prev_net_ref: Dict[str, Tuple[int, int, float]] = {}  # container.id -> (rx_total, tx_total, timestamp)

    if not args.json:
        print_table_header()

    iter_count = 0
    while not stop["flag"] and (args.iterations <= 0 or iter_count < args.iterations):
        containers = get_container_list(include_all=args.all, network=args.network)
        container_ids = [c["ID"] for c in containers if c.get("ID")]
        stats_by_id = get_stats(container_ids)
        details_by_id = get_container_details(container_ids)
        timestamp = time.time()

        for c in containers:
            container_id = c.get("ID")
            if not container_id:
                continue
            stats = stats_by_id.get(container_id)
            if not stats:
                # Non-running containers may appear in `docker ps -a` but not in `docker stats`.
                continue
            details = details_by_id.get(container_id, {})

            cpu = normalize_cpu_percent(stats["cpu_pct"], details.get("cpu_capacity", float(os.cpu_count() or 1)))
            mem_used = stats["mem_used"]
            mem_limit = stats["mem_limit"]
            mem_pct = stats["mem_pct"]
            storage_used = details.get("storage_used")
            storage_total = details.get("storage_total")
            storage_pct = details.get("storage_pct")
            rx_total = stats["rx_total"] or 0.0
            tx_total = stats["tx_total"] or 0.0
            rx_rate = tx_rate = None
            if container_id in prev_net_ref:
                prev_rx, prev_tx, prev_t = prev_net_ref[container_id]
                dt_secs = max(0.000001, timestamp - prev_t)
                rx_rate = (rx_total - prev_rx) / dt_secs
                tx_rate = (tx_total - prev_tx) / dt_secs
            prev_net_ref[container_id] = (rx_total, tx_total, timestamp)

            # Build output record
            sample = {
                "time": now_str(),
                "id": container_id[:12],
                "name": (c.get("Names") or container_id[:12]),
                "cpu_pct": cpu,
                "mem_used": mem_used,
                "mem_limit": mem_limit,
                "mem_pct": mem_pct,
                "storage_used": storage_used,
                "storage_total": storage_total,
                "storage_pct": storage_pct,
                "rx_total": rx_total,
                "tx_total": tx_total,
                "rx_rate": rx_rate,
                "tx_rate": tx_rate,
            }

            if args.json:
                # Emit NDJSON (one JSON object per line)
                print(json.dumps(sample, separators=(",", ":")))
            else:
                print_table_row(sample)

        iter_count += 1
        # Sleep until next tick if we are not stopping
        if args.iterations <= 0 or iter_count < args.iterations:
            # Honor the full interval relative to the start of the loop
            time.sleep(args.interval)

if __name__ == "__main__":
    try:
        main()
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError):
        print(
            "Docker command error. Make sure Docker is installed, the daemon is running, and your user can access it.\n"
            "On Linux: your user may need to be in the 'docker' group (then log out/in).",
            file=sys.stderr,
        )
        sys.exit(2)
