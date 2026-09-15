## This page is going to be updated

# SECBench

SECBench is a benchmarking framework for evaluating stream processing systems in the sensor-edge-cloud (SEC) continuum. It combines configurable SEC environment emulation, workload generation, system-under-test deployment, and performance tracking so that cloud-native and SEC processing models can be compared under reproducible hardware and network conditions.

The framework accompanies the paper:

> **SECBench: A Benchmarking Framework for Stream Processing Systems in the Sensor-Edge-Cloud Continuum**
> Taha Tekdogan, Lukas Schwerdtfeger, Tilmann Rabl, Steffen Zeuch, and Volker Markl.

## Purpose

Established stream processing benchmarks usually focus on centralized cloud-only deployments. SECBench targets deployments where data is generated at distributed sensor sources, may be processed on edge nodes, and can be forwarded to cloud resources. In this setting, benchmark results depend not only on the stream processing system and workload, but also on:

- hardware heterogeneity across sensor, edge, and cloud nodes
- network topology, latency, and bandwidth between nodes
- operator placement and processing model
- distributed source and sink placement
- end-to-end latency and resource utilization

SECBench makes these factors configurable and deploys them in a controlled container-based emulation environment.

## Architecture

SECBench follows the benchmark flow described in the paper:

1. Configure the SEC environment, including node resources and network links.
2. Construct the environment with containerized hosts and emulated links.
3. Deploy the stream processing system under test (SUT).
4. Start workload generators that publish streams to SUT sources.
5. Collect output events at the SUT sink.
6. Measure end-to-end latency and container resource usage.

The main components are:

- **Environment emulator**: builds configurable SEC topologies with Containernet/Mininet and Docker containers.
- **Data generator**: publishes synthetic or trace-driven streams over TCP, MQTT, or Kafka.
- **Result tracker**: timestamps output tuples and computes event latency.
- **Resource tracker**: samples CPU, memory, storage, and network usage for Docker containers.

## Repository Layout

```text
.
|-- datagen/                    # Parallel benchmark data generator
|-- emulator/                   # Containernet-based SEC environment emulator
|-- result-tracker/             # TCP latency tracker
|-- resource-tracker/           # Docker resource monitor
|-- README.md
`-- LICENSE
```

Useful entry points:

- `datagen/README.md`: data generator build, configuration, Docker, MQTT, and Kafka usage.
- `datagen/config.yaml`: example workload configuration.
- `emulator/config/config.yaml`: example hardware and network configuration.
- `emulator/examples/`: example Containernet deployments, including NebulaStream and Flink-oriented scripts.
- `result-tracker/README.MD`: latency tracker configuration and usage.
- `resource-tracker/result-tracker.py`: Docker resource monitoring script.

## Environment Configuration

SECBench models two SEC environment characteristics as first-class benchmark configuration:

- **Hardware heterogeneity**: CPU cores, memory, and storage limits per emulated node.
- **Network topology**: connectivity graph plus latency and bandwidth properties for links between nodes.

Conceptually, a SECBench environment configuration follows this structure:

```yaml
hardware:
  - id: node1
    cpu: 64
    memory: 512g
    storage: 2048g
  - id: node2
    cpu: 4
    memory: 8g
    storage: 16g

network:
  topology: custom
  links:
    - link: node1:node2
      latency: 15
      bandwidth: 200
```

This describes two heterogeneous hosts connected by a 15 ms, 200 Mbps link. Other benchmark setups can represent line, tree, ring, or custom SEC topologies.

## Data Generation

The data generator creates streaming workloads from configured data sources and temporal patterns. It supports:

- CSV input data
- image-folder input data
- uniform, Poisson, Zipfian, and burst patterns
- TCP socket output
- optional MQTT output
- optional Kafka output
- independent per-client streams or a shared stream across clients

Build the generator:

```bash
cd datagen
cmake -S . -B build
cmake --build build --target datagen datagen_shared
```

Run a generator with the default config:

```bash
./build/datagen config.yaml
```

Run the shared-stream variant:

```bash
./build/datagen_shared config.yaml
```

The Docker setup in `datagen/docker/` can also start the generator together with optional MQTT or Kafka services.

## Latency Tracking

The result tracker receives newline-delimited CSV tuples over TCP. It expects the original event timestamp as the last field, appends an ingress timestamp, and writes latency results.

Build and run:

```bash
cd result-tracker
make
./rtracker config.yaml
```

Example input tuple:

```text
1,2.0,5000,1774966532000
```

Example output format:

```text
8064526,1,2.0,5000,1774966532000,1774974596526
```

The first field is the computed latency in milliseconds.

## Resource Tracking

The resource tracker samples Docker container metrics during benchmark runs. It reports CPU, memory, network I/O, and storage-related information.

```bash
cd resource-tracker
python3 result-tracker.py
```

Useful options include:

```bash
python3 result-tracker.py -i 5 -n 12
python3 result-tracker.py --json
python3 result-tracker.py --network <docker-network>
```

## Emulation

For automated NebulaStream deployment from YAML, use
[`emulator/deploy_nebulastream.py`](emulator/deploy_nebulastream.py).
It creates workers, direct routed links, resource limits, and the NebulaStream CLI.
See the [deployment guide](emulator/NEBULASTREAM.md) and
[executable configuration](emulator/config/nebulastream.yaml).

```bash
python3 emulator/deploy_nebulastream.py --dry-run
sudo -E env PATH="$PATH" python3 emulator/deploy_nebulastream.py
```

The `emulator/` directory contains the Containernet-based environment layer and example deployments. Containernet requires Docker and privileged networking capabilities, so most examples need to be run with root privileges.

Typical execution pattern:

```bash
cd emulator
sudo -E env PATH=$PATH python3 examples/<example>.py
```

The paper evaluates SECBench with cloud-native and SEC processing models, including Flink/Kafka-style cloud processing and NebulaStream-style SEC processing. The example scripts under `emulator/examples/` provide starting points for those deployments.

## Benchmark Workflow

For a coordinated NebulaStream TCP benchmark, use
[`emulator/run_experiment.py`](emulator/run_experiment.py). It starts separately
resourced datagen and latency-tracker containers on routed links, submits the
query, releases generation after readiness, and collects results before cleanup.
See [the experiment guide](emulator/EXPERIMENTS.md) for image builds and the
[two-worker example](emulator/config/experiment.yaml).

```bash
python3 emulator/run_experiment.py --dry-run
sudo env -u PYTHONPATH emulator/venv/bin/python -B emulator/run_experiment.py
```

Other SUTs and the resource tracker still require separate orchestration steps:

1. Start the latency tracker at the configured SUT sink.
2. Start the resource tracker for the relevant Docker network or containers.
3. Start the emulated SEC topology and deploy the SUT.
4. Start one or more data generator instances against the configured SUT source endpoints.
5. Collect latency and resource output files after the run finishes.

Keep the data generator and trackers outside the SUT resource allocation when possible. This matches the paper's benchmark design and avoids measuring benchmark-driver overhead as SUT resource usage.

## Requirements

The exact requirements depend on the component being used:

- Linux with Docker
- privileged Docker/namespace/network access for Containernet-based emulation
- Python 3 for emulator scripts and resource tracking
- CMake and a C++17 compiler for `datagen`
- `make` and a C++17 compiler for `result-tracker`
- optional `librdkafka-dev` for Kafka output in `datagen`
- optional Mosquitto/MQTT components for MQTT output

## Citation

If you use SECBench in academic work, cite the paper:

```bibtex
@inproceedings{tekdogan2026secbench,
  title = {SECBench: A Benchmarking Framework for Stream Processing Systems in the Sensor-Edge-Cloud Continuum},
  author = {Tekdogan, Taha and Schwerdtfeger, Lukas and Rabl, Tilmann and Zeuch, Steffen and Markl, Volker},
  year = {2026}
}
```
