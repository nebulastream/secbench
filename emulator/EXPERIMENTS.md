# Automated TCP Experiments

`run_experiment.py` runs one shared TCP datagen, one NebulaStream query, and one
latency tracker. It reuses the environment launcher; the environment-only command
continues to work unchanged. It does not attach to an already running deployment.

The included five-second pass-through example constructs:

```text
datagen -- node1 -- node2 -- tracker
             |
           nebuli
```

All connections shown are direct routed emulation links. Worker links have 5 ms
one-way delay and 100 Mbps bandwidth. The generator/tracker attachment links have
zero added delay and 1 Gbps bandwidth. Datagen and tracker each have their own CPU
and memory limits, separate from workers. Their attachment delay contributes to
end-to-end latency. No benchmark ports are published through Docker's bridge.

## Build Images Once

Run from the repository root. The two base NebulaStream images must be available
locally. Building images requires network access to working package repositories;
the runner itself does not pull or build images.

```bash
docker build -f datagen/docker/Dockerfile -t secbench/datagen:local datagen
docker build -f emulator/docker/tracker.Dockerfile -t secbench/tracker:local .
docker build -f emulator/docker/nes-tools.Dockerfile \
  --build-arg BASE_IMAGE=nebulastream/worker:tt-tcp-sink \
  -t secbench/nes-worker:local emulator/docker
docker build -f emulator/docker/nes-tools.Dockerfile \
  --build-arg BASE_IMAGE=nebulastream/nes-cli:tt-tcp-sink \
  -t secbench/nes-cli:local emulator/docker
```

These images include the network tools, so the sample sets `install_tools: false`.
If a base image's APT repositories are unavailable, repair/rebuild that base image
before proceeding; disabling installation does not supply missing tools.

The runner requires the rebuilt `datagen_shared` executable with control protocol
version 1. An old image is rejected before query submission. The tracker build
uses the current `result-tracker/rtracker.cpp`, including local changes, without
modifying that component's config or source.

## Preview And Run

Use the Python environment containing the bundled Containernet dependencies:

```bash
env -u PYTHONPATH emulator/venv/bin/python -B emulator/run_experiment.py --dry-run
sudo env -u PYTHONPATH emulator/venv/bin/python -B emulator/run_experiment.py
```

The default config is [config/experiment.yaml](config/experiment.yaml). Select
another with `--config PATH`. Dry-run validation requires only Python and PyYAML;
it prints resolved component configs, query endpoints, resource limits, and routes
without Docker access. Actual execution requires the same privileged Linux host
setup as [environment deployment](NEBULASTREAM.md).

Use `--output-dir PATH` for a custom, nonexisting artifact directory. Otherwise a
new directory is created under `emulator/runs/`. Existing conflicting containers
are refused, never removed. Stop with Ctrl+C; this triggers bounded cleanup.

## Configuration

`hardware`, `network`, and `deployment` have the environment launcher's schema.
The additional `experiment` mapping contains:

| Key | Meaning |
| --- | --- |
| `duration_seconds` | Generation duration after release; default 10 |
| `startup_timeout` | Readiness and CLI command timeout; default 90 seconds |
| `drain_timeout` | Maximum wait for natural query completion and sink EOF; default 60 seconds |
| `shutdown_timeout` | Component termination grace period; default 15 seconds |
| `datagen.config` | Existing generator YAML file |
| `datagen.dataset` | Host CSV file or image directory, mounted read-only at `/dataset` |
| `tracker.config` | Existing tracker YAML file |
| `query.template` | Query YAML containing SQL, source and sink schemas |

Both component entries support `image`, `attach_to`, `cpu`, `memory`, `port`, and
`link`. `attach_to` must name a worker. Their default resources are 1 CPU and 1 GB
each; defaults for `port` are 8082 and 9000. Attachment `link` uses the same
`latency`, `bandwidth`, and `loss` fields as worker links. With no settings, that
attachment has no added shaping. `datagen` and `tracker` are reserved node names
in experiment mode. All input paths are relative to the experiment config.

Original files are never rewritten. Resolved copies override:

- Datagen bind address, source port, dataset path, and duration.
- Tracker server address/port, run-local output paths, 100% sampling, full latency
  rows, and per-second tuple counts. Per-tuple terminal printing is disabled.
- Query physical-source placement and endpoint, TCP sink placement and endpoint,
  and worker topology. Placement follows a tree rooted at the tracker attachment;
  physical routing retains the full configured worker graph.

The first version supports exactly one TCP physical source, one logical source,
and one TCP sink with newline-delimited CSV. MQTT/Kafka and sequence-header output
must be disabled. The query must preserve the original epoch-millisecond timestamp
as its final output field, with the same name as the final source field. Schema
validation cannot prove arbitrary SQL preserves timestamp semantics; the included
pass-through query does. Explicit SQL aliases are needed by the supplied CLI
image to match sink field names. Model paths still use `deployment.model_dir`.

## Lifecycle And Results

1. Validate inputs and local images; construct workers, services, and routes.
2. Start the tracker and inspect its listening socket.
3. Start datagen with listeners active but generation paused.
4. Submit the resolved query and retain its persisted CLI state.
5. Require global query status `Running` and established source/sink connections.
6. Release datagen; its duration timer starts only after release and client acceptance.
7. Wait for datagen to exit, then for natural query status `Stopped` and sink EOF.
8. Terminate the tracker gracefully, allowing queued results to drain, validate
   nonempty latency output, and tear down the environment.

Readiness inspection never opens a disposable datagen connection. A timeout or
component failure marks the run failed; forced query termination is not treated
as successful draining. Failure cleanup stops generation, attempts query shutdown,
and then stops the tracker before environment teardown. SIGKILL cannot clean up.
The resource tracker is not started by this runner.

Artifacts include `deployment.json`, `experiment.yaml`, `summary.json`, worker
logs, resolved component/query YAML, component logs and exit codes, CLI state and
command logs, tracker spool, latency output, and consumed-tuple counts.
`summary.json` records lifecycle timestamps, query ID, image IDs, result-row count,
and errors. Success requires nonempty rows with internally consistent, nonnegative
latencies; this is not a proof of application-level tuple completeness.

## Verification

Root-free unit tests:

```bash
env -u PYTHONPATH emulator/venv/bin/python -B -m unittest discover -s emulator/tests -v
```

Optional native TCP and real two-worker CLI tests:

```bash
cmake -S datagen -B /tmp/secbench-datagen-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/secbench-datagen-build --target datagen_shared -j2
g++ -std=c++17 -O2 -pthread result-tracker/rtracker.cpp -o /tmp/secbench-rtracker
env -u PYTHONPATH \
  SECBENCH_DATAGEN_BINARY=/tmp/secbench-datagen-build/datagen_shared \
  SECBENCH_TRACKER_BINARY=/tmp/secbench-rtracker \
  SECBENCH_LIVE_CLI=1 \
  emulator/venv/bin/python -B -m unittest discover -s emulator/tests -v
```

The live CLI test creates and removes its own two worker containers on Docker's
bridge and runs the native components on the host. It validates the real query
protocol, gate, natural completion, and latency output, but **not Mininet routing
or traffic shaping**. Full routed verification requires running the experiment
command with sudo after building the prepared images.
