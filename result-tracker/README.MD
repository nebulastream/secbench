# Result Tracker

`rtracker` is a TCP-based latency tracker for stream-processing benchmarks.

It performs two tasks:

1. It accepts incoming CSV tuples over TCP and immediately appends the current system timestamp in milliseconds.
2. It computes latency by subtracting the original tuple timestamp from the assigned ingress timestamp, then writes the result to a file and prints it to the terminal.

The original timestamp is expected to be the last field in each incoming tuple.

Example input tuple:

```text
1,2.0,5000,1774966532000
```

After ingress timestamping, the staged tuple looks like:

```text
1,2.0,5000,1774966532000,1774974596526
```

The latency output format is:

```text
8064526,1,2.0,5000,1774966532000,1774974596526
```

The first value is the computed latency in milliseconds.

## Runtime Model

The tracker uses a two-stage pipeline:

- TCP handler threads receive newline-delimited CSV tuples and attach the current timestamp as quickly as possible.
- Timestamped tuples are appended to a spool file on disk.
- A spool reader thread forwards staged tuples to latency worker threads.
- Latency worker threads compute the latency and write the result to the configured output file and standard output.

This design prioritizes timestamp attachment on ingress and decouples it from latency calculation work.

## Files

- `rtracker.cpp`: main implementation
- `config.yaml`: runtime configuration
- `timestamped_tuples.log`: staged tuples written during runtime
- `latency_output.log`: computed latency output

## Configuration

The tool reads `config.yaml` by default. You can also pass a custom config path as the first command-line argument.

Current supported transport:

- `tcp`

Example configuration:

```yaml
connection:
  type: tcp
  mode: server
  host: 0.0.0.0
  port: 9000
  backlog: 64
  read_buffer_bytes: 4096

processing:
  sample: 100
  latency_worker_threads: 2
  spool_file: timestamped_tuples.log
  latency_output_file: latency_output.log
  print_to_terminal: true
  print_only_latency: false
  print_consumed_tuples: false
```

Configuration fields:

- `connection.type`: must be `tcp`
- `connection.mode`: either `server` to listen for incoming connections or `client` to connect to a remote TCP endpoint
- `connection.host`: local bind address in server mode, remote IPv4 address in client mode
- `connection.port`: TCP listen port in server mode, remote TCP port in client mode
- `connection.backlog`: socket listen backlog
- `connection.read_buffer_bytes`: per-client receive buffer size
- `processing.sample`: percentage of received tuples to timestamp and process, from `0` to `100`
- `processing.latency_worker_threads`: number of latency calculation threads
- `processing.spool_file`: file used to stage timestamped tuples
- `processing.latency_output_file`: file used for computed latency output
- `processing.print_to_terminal`: whether to print latency lines to stdout
- `processing.print_only_latency`: when `true`, output only the calculated latency value for valid tuples; when `false`, keep the full `latency,staged_tuple` output
- `processing.print_consumed_tuples`: when `true`, write one line per second to `consumed_tuples.txt` with the current timestamp and number of received tuples in that second

## Compilation

Build with:

```bash
g++ -std=c++17 -O3 -pthread rtracker.cpp -o rtracker
```

Or with `make`:

```bash
make
```

## Usage

Start the tracker:

```bash
./rtracker config.yaml
```

If no argument is provided, the program still tries to load `config.yaml` from the current working directory:

```bash
./rtracker
```

When `connection.mode` is `server`, the tracker listens on the configured TCP address and port. When it is `client`, the tracker connects to the configured TCP address and port and reads newline-delimited tuples from that connection.

## Docker

Build the image:

```bash
docker build -t result-tracker .
```

Run the container with the default config and expose the TCP port:

```bash
docker run --rm -p 9000:9000 result-tracker
```

If you want the generated files on the host, mount the project directory into the container:

```bash
docker run --rm -p 9000:9000 \
  -v "$(pwd):/app" \
  result-tracker ./config.yaml
```

Notes:

- `-p 9000:9000` must match `connection.port` in `config.yaml`.
- Mounting `$(pwd)` lets the container use your local `config.yaml` and write `timestamped_tuples.log` and `latency_output.log` into the current directory.
- If you change the config file name, pass it as the container command argument.

## Sending Test Data

You can send a tuple locally with bash:

```bash
printf '1,2.0,5000,1774966532000\n' >/dev/tcp/127.0.0.1/9000
```

Input expectations:

- Each tuple must be on its own line.
- Tuples must be valid CSV-like comma-separated records.
- The last field must be the original event timestamp in milliseconds since epoch.

## Output

During runtime, the tracker writes:

- staged timestamped tuples to the configured spool file
- computed latency lines to the configured output file
- the same latency lines to the terminal when `print_to_terminal` is enabled
- per-second consumed tuple counts to `consumed_tuples.txt` when `print_consumed_tuples` is enabled

## Shutdown

Stop the server with `Ctrl+C`.

The program handles `SIGINT` and `SIGTERM`, closes the listening socket, finishes pending work, and joins worker threads before exit.
