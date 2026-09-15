#!/usr/bin/env python3
"""Run a configured NebulaStream TCP benchmark and retain its artifacts."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import sys

import deploy_nebulastream as deployment
from nebulastream_environment import ConfigError
from nebulastream_experiment import load_experiment


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=deployment.ROOT / "config/experiment.yaml")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)
    experiment = None
    previous = signal.signal(signal.SIGTERM, deployment.interrupted)
    try:
        plan, experiment = load_experiment(args.config)
        if args.dry_run:
            print(json.dumps({"environment": plan.as_dict(), "experiment": experiment.settings,
                              "resolved": experiment.configs}, indent=2))
            return 0
        output = args.output_dir or deployment.ROOT / "runs" / (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}")
        if ":" in str(output.resolve()):
            raise ConfigError("Output directory must not contain ':'")
        deployment.run(plan, output.resolve(), lifecycle=experiment)
        return 0
    except (KeyboardInterrupt, Exception) as exc:
        if experiment is not None and hasattr(experiment, "output"):
            experiment.summary["error"] = str(exc) or type(exc).__name__
            experiment.event("interrupted" if isinstance(exc, KeyboardInterrupt) else "failed")
        print(f"Experiment failed: {exc}", file=sys.stderr)
        return 130 if isinstance(exc, KeyboardInterrupt) else 1
    finally:
        signal.signal(signal.SIGTERM, previous)


if __name__ == "__main__":
    sys.exit(main())
