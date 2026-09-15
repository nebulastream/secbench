"""Configuration and lifecycle for one TCP datagen/query/tracker experiment."""

from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import time

import yaml

import deploy_nebulastream as deployment
from nebulastream_environment import ConfigError, make_plan, mapping, number


def read_yaml(path):
    try:
        value = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"Cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return value


def input_path(base, value, label):
    if not isinstance(value, str) or not value or ":" in value:
        raise ConfigError(f"{label} must be a path without ':'")
    path = (base / value).resolve()
    if not path.exists():
        raise ConfigError(f"{label} does not exist: {path}")
    return path


def object_config(value, label):
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be a mapping")
    return value


def resolve_query(template, plan, settings):
    query = deepcopy(template)
    if not isinstance(query.get("query"), str) or not query["query"].strip():
        raise ConfigError("query template must contain a single SQL string")
    for key in ("physical", "logical", "sinks"):
        if not isinstance(query.get(key), list) or len(query[key]) != 1 or not isinstance(query[key][0], dict):
            raise ConfigError(f"First runner requires exactly one {key} entry")
    source, logical, sink = query["physical"][0], query["logical"][0], query["sinks"][0]
    if source.get("type") != "TCP" or sink.get("type") != "TCP":
        raise ConfigError("First runner supports TCP sources and sinks only")
    parser = source.get("parser_config", {})
    if not isinstance(parser, dict) or parser.get("type") != "CSV" or parser.get("field_delimiter") != "," \
            or parser.get("tuple_delimiter") not in ("\n", "\\n"):
        raise ConfigError("First runner requires newline-delimited CSV with comma-separated fields")
    # This CLI version unescapes delimiter strings after YAML parsing.
    parser["tuple_delimiter"] = "\\n"
    if not isinstance(logical.get("name"), str) or not logical["name"] or source.get("logical") != logical["name"]:
        raise ConfigError("Physical source must reference the logical source")
    for entry in (logical, sink):
        schema = entry.get("schema")
        if not isinstance(schema, list) or not schema or not isinstance(schema[-1], dict):
            raise ConfigError("Source and sink schemas must end in an epoch-millisecond timestamp")
        if not isinstance(schema[-1].get("name"), str) or not schema[-1]["name"]:
            raise ConfigError("Final timestamp fields must have nonempty names")
        if schema[-1].get("type") not in ("UINT64", "INT64"):
            raise ConfigError("Final source and sink fields must be integer timestamps")
    if sink["schema"][-1]["name"] != logical["schema"][-1]["name"]:
        raise ConfigError("Source and sink must preserve the same final timestamp field")
    workers = {host.id: host for host in plan.workers}
    services = {host.id: host for host in plan.services}
    grpc = lambda identifier: f"{workers[identifier].ip}:{plan.deployment['grpc_port']}"
    source["host"] = grpc(settings["datagen"]["attach_to"])
    source["source_config"] = object_config(source.get("source_config", {}), "source_config") | {
        "socket_host": services["datagen"].ip, "socket_port": str(settings["datagen"]["port"]),
    }
    sink["host"] = grpc(settings["tracker"]["attach_to"])
    sink["config"] = object_config(sink.get("config", {}), "sink.config") | {
        "socket_host": services["tracker"].ip, "socket_port": str(settings["tracker"]["port"]),
        "output_format": "CSV",
    }
    # NES placement uses an acyclic graph rooted at the sink. Physical routing
    # still follows the full configured worker graph, including any cycles.
    root = settings["tracker"]["attach_to"]
    adjacent = {identifier: [] for identifier in workers}
    for link in plan.links:
        if link.left in workers and link.right in workers:
            adjacent[link.left].append(link.right)
            adjacent[link.right].append(link.left)
    parents, pending = {root: None}, deque([root])
    while pending:
        current = pending.popleft()
        for neighbor in adjacent[current]:
            if neighbor not in parents:
                parents[neighbor] = current
                pending.append(neighbor)
    query["workers"] = []
    for identifier, host in workers.items():
        item = {"host": grpc(identifier), "data_address": f"{host.ip}:{plan.deployment['data_port']}",
                "max_operators": 10000}
        if parents[identifier] is not None:
            item["downstream"] = [grpc(parents[identifier])]
        query["workers"].append(item)
    return query


def load_experiment(path):
    path = Path(path).resolve()
    config = read_yaml(path)
    settings = deepcopy(mapping(config.pop("experiment", None), "experiment",
        "datagen tracker query duration_seconds startup_timeout drain_timeout shutdown_timeout"))
    for key, default in (("duration_seconds", 10), ("startup_timeout", 90),
                         ("drain_timeout", 60), ("shutdown_timeout", 15)):
        settings[key] = number(settings.get(key, default), key, 1, 86400, integer=True)
    for role, image, port in (("datagen", "secbench/datagen:local", 8082),
                              ("tracker", "secbench/tracker:local", 9000)):
        entry = dict(mapping(settings.get(role), role, "config image attach_to cpu memory link port dataset"))
        if role == "tracker" and "dataset" in entry:
            raise ConfigError("tracker.dataset is not supported")
        entry.setdefault("image", image)
        if not isinstance(entry["image"], str) or not entry["image"].strip():
            raise ConfigError(f"{role}.image must be a nonempty string")
        entry["port"] = number(entry.get("port", port), f"{role}.port", 1, 65535, integer=True)
        if not isinstance(entry.get("attach_to"), str):
            raise ConfigError(f"{role}.attach_to must name a worker")
        entry["config"] = str(input_path(path.parent, entry.get("config"), f"{role}.config"))
        settings[role] = entry
    plan = make_plan(config, path.parent, {role: settings[role] for role in ("datagen", "tracker")})
    query_settings = mapping(settings.get("query"), "experiment.query", "template")
    query_path = input_path(path.parent, query_settings.get("template"), "query.template")
    settings["query"] = {"template": str(query_path)}
    datagen = read_yaml(Path(settings["datagen"]["config"]))
    for transport in ("mqtt", "kafka"):
        if object_config(datagen.get(transport, {}), f"datagen.{transport}").get("enabled", False):
            raise ConfigError("First runner requires MQTT and Kafka to be disabled")
    if object_config(datagen.get("slow_consumer", {}), "slow_consumer").get("sequence_header", False):
        raise ConfigError("Sequence headers are incompatible with the CSV query")
    data_source = datagen.get("data_source", {})
    if not isinstance(data_source, dict) or data_source.get("type", "csv") not in ("csv", "image_folder"):
        raise ConfigError("datagen.data_source.type must be csv or image_folder")
    dataset = input_path(path.parent, settings["datagen"].get("dataset"), "datagen.dataset")
    if (data_source.get("type", "csv") == "csv" and not dataset.is_file()) or (
            data_source.get("type") == "image_folder" and not dataset.is_dir()):
        raise ConfigError("datagen.dataset does not match data_source.type")
    settings["datagen"]["dataset"] = str(dataset)
    datagen["data_source"] = data_source | {"file": "/dataset"}
    datagen.update(host=next(h.ip for h in plan.services if h.id == "datagen"),
                   ports=[settings["datagen"]["port"]], duration=settings["duration_seconds"])
    tracker = read_yaml(Path(settings["tracker"]["config"]))
    tracker["connection"] = object_config(tracker.get("connection", {}), "tracker.connection") | {
        "type": "tcp", "mode": "server", "host": next(h.ip for h in plan.services if h.id == "tracker"),
        "port": settings["tracker"]["port"],
    }
    tracker["processing"] = object_config(tracker.get("processing", {}), "tracker.processing") | {
        "sample": 100, "spool_file": "/run/secbench/timestamped_tuples.log",
        "latency_output_file": "/run/secbench/latency_output.log",
        "print_to_terminal": False, "print_only_latency": False, "print_consumed_tuples": True,
    }
    query = resolve_query(read_yaml(query_path), plan, settings)
    return plan, Experiment(settings, datagen, tracker, query)


def query_status(text, query_id):
    rows = json.loads(text)
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("CLI returned no query status rows")
    for row in rows:
        if not isinstance(row, dict) or row.get("query_id") != query_id or not row.get("query_status"):
            raise RuntimeError("Unexpected CLI status schema or query ID")
        if row.get("error") or row["query_status"] in ("Failed", "ConnectionError"):
            raise RuntimeError(f"Query failed: {row}")
    global_rows = [row for row in rows if row.get("local_query_id") is None and row.get("worker") is None]
    if len(global_rows) != 1:
        raise RuntimeError("CLI must report exactly one global query status")
    return global_rows[0]["query_status"]


class Experiment:
    def __init__(self, settings, datagen, tracker, query):
        self.settings = settings
        self.configs = {"datagen": datagen, "tracker": tracker, "query": query}
        self.summary = {"status": "preparing", "events": [], "exit_codes": {}}
        self.query_id = None
        self.started = []

    def event(self, state, **details):
        self.summary["status"] = state
        self.summary["events"].append({"state": state, "time": datetime.now(timezone.utc).isoformat(), **details})
        (self.output / "summary.json").write_text(json.dumps(self.summary, indent=2) + "\n")
        print(f"Experiment: {state}", flush=True)

    def prepare(self, plan, output):
        self.output = output
        self.plan = plan
        self.hosts = {host.id: host for host in plan.services}
        for directory in ("datagen", "tracker", "query", "state", "control"):
            (output / directory).mkdir()
        for role, config in self.configs.items():
            (output / role / "config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
        (output / "experiment.yaml").write_text(yaml.safe_dump(self.settings, sort_keys=False))
        plan.mounts.update({
            "datagen": [f"{output / 'datagen'}:/run/secbench:rw",
                        f"{self.settings['datagen']['dataset']}:/dataset:ro", f"{output / 'control'}:/control:ro"],
            "tracker": [f"{output / 'tracker'}:/run/secbench:rw"],
            plan.cli.id: [f"{output / 'query'}:/experiment:ro", f"{output / 'state'}:/state:rw"],
        })
        images = {plan.deployment["worker_image"], plan.deployment["cli_image"],
                  self.settings["datagen"]["image"], self.settings["tracker"]["image"]}
        self.summary["images"] = {image: json.loads(deployment.command(
            ["docker", "image", "inspect", image]).stdout)[0]["Id"] for image in sorted(images)}
        self.event("preparing")

    def launch(self, role, argv):
        command = shlex.join(argv)
        script = (f"cd /run/secbench || exit 1; {command} & child=$!; "
                  'echo "$child" > /run/secbench/process.pid; wait "$child"; '
                  'code=$?; echo "$code" > /run/secbench/process.exit; exit "$code"')
        # The supervisor persists exit status even after the child disappears.
        deployment.inside(self.hosts[role], f"nohup bash -c {shlex.quote(script)} "
                          "> /run/secbench/process.log 2>&1 < /dev/null &")
        self.started.append(role)

    def exit_code(self, role):
        path = self.output / role / "process.exit"
        if not path.exists():
            return None
        text = path.read_text().strip()
        if not text:
            return None
        code = int(text)
        self.summary["exit_codes"][role] = code
        return code

    def healthy(self):
        for role in self.started:
            code = self.exit_code(role)
            if code is not None and (role == "tracker" or code != 0):
                raise RuntimeError(f"{role} exited unexpectedly ({code}); see {role}/process.log")
        for host in self.plan.workers:
            deployment.inside(host, 'kill -0 "$(cat /tmp/secbench-worker.pid)"')

    def sockets(self, role, state):
        # Inspect sockets without consuming a source connection or generating tuples.
        result = deployment.inside(self.hosts[role],
            f"ss -H -n -t state {state} '( sport = :{self.settings[role]['port']} )'")
        return bool(result.stdout.strip())

    def sink_drained(self):
        result = deployment.inside(self.hosts["tracker"],
            f"ss -H -n -t state all '( sport = :{self.settings['tracker']['port']} )'")
        # CLOSE-WAIT can still contain unread bytes. Wait until application
        # sockets close; the listener and kernel-only TIME-WAIT are harmless.
        return all(line.split()[0] in ("LISTEN", "TIME-WAIT", "CLOSED")
                   for line in result.stdout.splitlines() if line.strip())

    def wait(self, description, predicate, timeout):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.healthy()
            if predicate():
                return
            time.sleep(0.2)
        raise RuntimeError(f"Timed out waiting for {description}")

    def cli(self, action, *args):
        result = deployment.inside(self.plan.cli, shlex.join([
            "env", "XDG_STATE_HOME=/state", "nes-cli", "-t", "/experiment/config.yaml", action, *args,
        ]), check=False, timeout=self.settings["startup_timeout"])
        with (self.output / "query" / "cli.log").open("a") as log:
            log.write(f"{action} {args}\n{result.stdout}\n{result.stderr}\n")
        if result.returncode:
            raise RuntimeError(f"nes-cli {action} failed; see query/cli.log")
        return result.stdout

    def status(self):
        return query_status(self.cli("status", self.query_id), self.query_id)

    def stop_process(self, role):
        if role not in self.started or self.exit_code(role) is not None:
            return
        deployment.inside(self.hosts[role], 'test ! -s /run/secbench/process.pid || '
                          'kill -TERM "$(cat /run/secbench/process.pid)"', check=False)
        deadline = time.monotonic() + self.settings["shutdown_timeout"]
        while time.monotonic() < deadline:
            if self.exit_code(role) is not None:
                return
            time.sleep(0.1)
        raise RuntimeError(f"{role} did not exit gracefully before shutdown timeout")

    def validate_results(self):
        count = 0
        with (self.output / "tracker" / "latency_output.log").open() as output:
            for line in output:
                fields = line.strip().split(",")
                if len(fields) < 4:
                    raise RuntimeError("Malformed latency result")
                latency, source, received = int(fields[0]), int(fields[-2]), int(fields[-1])
                if latency < 0 or received - source != latency:
                    raise RuntimeError("Invalid timestamp or latency result")
                count += 1
        if not count:
            raise RuntimeError("Query produced no valid latency results")
        self.summary["result_rows"] = count

    def execute(self, plan, output):
        error = None
        try:
            version = deployment.inside(self.hosts["datagen"], "datagen_shared --secbench-control-version").stdout.strip()
            if version != "1":
                raise RuntimeError("Rebuild the datagen image: coordinated startup version 1 is required")
            for host in self.hosts.values():
                deployment.inside(host, "command -v ss >/dev/null")
            self.event("starting_tracker")
            self.launch("tracker", ["rtracker", "/run/secbench/config.yaml"])
            self.wait("tracker listener", lambda: self.sockets("tracker", "listening"), self.settings["startup_timeout"])
            self.event("preparing_datagen")
            self.launch("datagen", ["env", "SECBENCH_START_FILE=/control/start", "datagen_shared", "/run/secbench/config.yaml"])
            self.wait("datagen listener", lambda: self.sockets("datagen", "listening"), self.settings["startup_timeout"])
            self.event("submitting_query")
            self.cli("start")
            states = list((output / "state" / "nebucli").glob("*.json"))
            if len(states) != 1:
                raise RuntimeError("Expected exactly one persisted query after submission")
            self.query_id = json.loads(states[0].read_text())["query_id"]
            self.summary["query_id"] = self.query_id
            self.wait("running query and source/sink connections", lambda: self.status() == "Running"
                      and self.sockets("datagen", "established") and self.sockets("tracker", "established"),
                      self.settings["startup_timeout"])
            self.event("running")
            (output / "control" / "start").touch(exist_ok=False)
            self.wait("datagen completion", lambda: self.exit_code("datagen") == 0,
                      self.settings["duration_seconds"] + self.settings["shutdown_timeout"])
            self.event("draining")
            self.wait("natural query completion and sink EOF", lambda: self.status() == "Stopped"
                      and self.sink_drained(), self.settings["drain_timeout"])
            self.stop_process("tracker")
            if self.exit_code("tracker") != 0:
                raise RuntimeError("Tracker did not exit successfully")
            self.validate_results()
        except BaseException as exc:
            error = exc
        finally:
            cleanup_errors = []
            # Stop generation first; keep the tracker alive while stopping the query.
            for action in (lambda: self.stop_process("datagen"),
                           lambda: self.cli("stop", self.query_id) if self.query_id and error else None,
                           lambda: self.stop_process("tracker")):
                try:
                    action()
                except Exception as exc:
                    cleanup_errors.append(str(exc))
            if cleanup_errors:
                self.summary["cleanup_errors"] = cleanup_errors
                error = error or RuntimeError("Experiment cleanup was incomplete")
            if error:
                self.summary["error"] = str(error) or type(error).__name__
            self.event("interrupted" if isinstance(error, KeyboardInterrupt) else "failed" if error else "completed")
        if error:
            raise error
