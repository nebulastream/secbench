# Configuration-Driven NebulaStream Deployment

`deploy_nebulastream.py` constructs an environment using the bundled Containernet
implementation. Each configured node is a Docker container running a NebulaStream
worker. Configured links are direct, bidirectional, routed connections between
containers. No switches or external OpenFlow controller are used.

This launcher deploys the environment and workers only. For coordinated query,
datagen, and latency-tracker execution, use the [experiment runner](EXPERIMENTS.md).

## Run

From the SECBench repository root, validate and inspect the expanded configuration:

```bash
python3 emulator/deploy_nebulastream.py --config emulator/config/nebulastream.yaml --dry-run
```

Dry runs require Python 3.10+ and PyYAML, but do not access Docker or require root.
The default configuration path is resolved relative to the launcher, so omitting
`--config` works from any working directory.

For deployment, use a Python environment with the dependencies listed in
`emulator/pyproject.toml` and the host dependencies described in
[the Containernet installation instructions](README.md#installation). Docker,
`mnexec`, and Linux network namespace/traffic-control support must be available.
The container images must already exist locally and provide Bash and the worker
or CLI binary. The sample configurations enable `install_tools`, which installs
missing `iproute2`, `net-tools`, `procps`, `iputils-ping`, and `coreutils` packages
inside the containers before network setup. This needs working APT repositories
and network access from the images. For repeated or large experiments, include
these packages in your images and disable `install_tools`. The launcher uses the
bundled `mininet` package.

```bash
sudo -E env PATH="$PATH" python3 emulator/deploy_nebulastream.py --config emulator/config/nebulastream.yaml
```

After workers become reachable from `nebuli`, the Mininet CLI opens. For example:

```text
mininet> nebuli ping -c 3 node3
mininet> node1 ip route
mininet> node2 tc qdisc show dev node2-eth1
mininet> node3 tail -n 30 /tmp/secbench-worker.log
mininet> exit
```

An automated startup/connectivity check creates the environment and tears it down:

```bash
sudo -E env PATH="$PATH" python3 emulator/deploy_nebulastream.py --smoke-test
```

With the repository's existing virtual environment, avoid inherited Python path
overrides when running the smoke test:

```bash
sudo env -u PYTHONPATH emulator/venv/bin/python -B emulator/deploy_nebulastream.py --smoke-test
```

Use `--headless` to keep the environment running until SIGINT/SIGTERM. Normal CLI
exit, signals, and startup failures trigger cleanup. Existing containers with
conflicting names cause an error instead of being removed. Forced termination
with SIGKILL cannot run cleanup.

Each run writes an expanded `deployment.json`, including worker endpoints and
routes, to a new directory beneath `emulator/runs/`. Worker logs are collected
there on exit. `--output-dir PATH` selects a different, new directory. The directory
must not already exist, to avoid overwriting earlier results.

## Configuration

Use [nebulastream.yaml](config/nebulastream.yaml) as the executable example.
The earlier `config/config.yaml` is a legacy, nonstandard YAML sketch consumed by
the older examples; it is not the new launcher's schema.

```yaml
hardware:
  - id: node1
    cpu: 2
    memory: 2g
  - id: node2
    cpu: 0.5
    memory: 1g

network:
  topology: custom
  subnet: 10.1.1.0/24
  link_subnet: 10.200.0.0/16
  links:
    - link: node1:node2
      latency: 15
      bandwidth: 200
  cli_node: node1
```

- `hardware` specifies workers in address-allocation order. Each entry requires
  `id`, positive `cpu` (fractional cores allowed), and `memory` such as `512m` or
  `2g`. IDs are unique, 1-7 characters, starting with a letter; remaining characters
  may be letters, numbers, `_`, or `-`. `nebuli` is reserved.
- Optional `count` expands an entry, e.g. `id: node, count: 64` becomes `node1`
  through `node64`, all with the same resource profile.
- Optional `storage: 16g` requests Docker writable-layer size enforcement. This
  requires a supporting Docker storage driver/filesystem. An unsupported request
  fails deployment; the launcher never silently drops resource limits. This is
  not a quota on bind-mounted datasets or queries. Omit it when not needed.
- `network.subnet` supplies stable service IPs: workers receive successive
  addresses starting at `.1`; `nebuli` receives the last usable address. Each
  service IP is installed as a `/32` on the container's loopback interface.
- `network.link_subnet` supplies a distinct `/30` for each link. The service
  and link pools must not overlap each other or Docker's management bridge.
- `latency` is one-way delay in milliseconds, `bandwidth` is Mbps per direction,
  and optional `loss` is a percentage. Unspecified delay/loss are zero;
  unspecified bandwidth is unconstrained. Values apply at each transmitting
  end of a link: two 15 ms hops add about 30 ms one way and 60 ms RTT.
- `default_link` provides defaults for worker links, which custom links can
  override. `cli_node` selects the worker to which `nebuli` attaches (default:
  first worker). `cli_link` independently configures that attachment.

Nodes forward packets along static minimum-hop routes. Equal-length paths use
configuration link order to break ties. Routing is not latency-aware and does not
recompute after a link failure. Intermediate workers perform IP forwarding within
their resource allocation. `nebuli` traffic traverses the same routed topology;
there is no separate emulated management shortcut.
Forwarding and reverse-path filtering are configured through Docker's startup
sysctls, since `/proc/sys` is read-only inside the containers. Their values are
checked again during network setup.

The Docker bridge remains available for host management and optional published
gRPC ports. Use the service IPs/hostnames in worker topology definitions to keep
SUT traffic on the shaped links. Worker data ports bind to their routed service
IPs and are not published through Docker. Published gRPC ports bypass emulated
link shaping and are for management only.

## Topology Templates

Custom links can express any connected simple worker graph, including cycles.
The templates use worker order from `hardware`:

| `topology` | Worker Links |
| --- | --- |
| `custom` | Exactly the pairs in `network.links` |
| `line` | Each worker connects to the next |
| `star` | First worker connects to every other worker |
| `ring` | Line plus a last-to-first link; at least three workers |
| `tree` | Breadth-first tree with `branching_factor` children per worker (default 2) |

For example, [nebulastream-scaled.yaml](config/nebulastream-scaled.yaml) expands
to 64 workers in a four-way routed tree. It follows the scale and resource
profiles of `nes-scaled.py`, but uses worker routing rather than its switch tree.
Preview it before deployment, since its aggregate resource demand is substantial:

```bash
python3 emulator/deploy_nebulastream.py --config emulator/config/nebulastream-scaled.yaml --dry-run
```

## Deployment Options

The optional `deployment` mapping controls:

| Key | Default / Meaning |
| --- | --- |
| `worker_image` | `nebulastream/worker:tt-tcp-sink` |
| `cli_image` | `nebulastream/nes-cli:tt-tcp-sink` |
| `worker_binary` | `nes-single-node-worker` |
| `worker_args` | List of additional argument strings; default empty |
| `grpc_port`, `data_port` | 8080 and 9090 inside each worker |
| `startup_timeout` | 90 seconds for TCP readiness from `nebuli` after launching workers |
| `publish_ports` | `false`; enable sequential host gRPC port bindings |
| `host_grpc_port_base` | 18080 |
| `install_tools` | `false`; install missing network utilities with APT before setup |
| `cli_cpu`, `cli_memory` | 0.5 CPU and `512m` |
| `queries_dir` | Optional directory mounted read-only at `/queries` in `nebuli` |
| `model_dir` | Optional directory staged read-only and copied to `/model` in all containers |

Paths are relative to the YAML file. Network endpoint arguments are generated by
the launcher and must not be included in `worker_args`. The sample config uses
the extra worker arguments from `dist-nes.py`.

Readiness checks establish TCP connections to worker gRPC ports; they do not
register or execute a query. All nodes share one physical host's resources and
architecture. CPU limits are enforced individually, but users must provision
enough aggregate CPU, memory, and disk for their experiment.

## Tests

The focused tests use fake emulation objects and do not create containers:

```bash
python3 -B -m unittest discover -s emulator/tests -v
```

If the inherited `PYTHONPATH` shadows your virtual environment's dependencies,
run with `env -u PYTHONPATH` and the environment's Python executable. The Docker
SDK regression test needs the dependencies in `emulator/pyproject.toml`; it is
skipped when they are unavailable.
