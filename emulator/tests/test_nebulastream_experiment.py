import contextlib
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import deploy_nebulastream as deployment
from nebulastream_environment import ConfigError
from nebulastream_experiment import load_experiment, query_status, resolve_query
import run_experiment


class ExperimentTests(unittest.TestCase):
    def setUp(self):
        self.plan, self.experiment = load_experiment(ROOT / "config/experiment.yaml")

    def test_service_routes_and_resolved_query(self):
        self.assertEqual([host.id for host in self.plan.workers], ["node1", "node2"])
        self.assertEqual([host.id for host in self.plan.services], ["datagen", "tracker"])
        self.assertEqual(len(self.plan.routes), 20)
        routes = {(r["host"], r["destination"]): r for r in self.plan.routes}
        self.assertEqual(routes["datagen", "10.1.1.4"]["gateway"], "10.200.0.6")
        self.assertEqual(routes["node1", "10.1.1.4"]["gateway"], "10.200.0.2")
        query = self.experiment.configs["query"]
        self.assertEqual(query["physical"][0]["source_config"]["socket_host"], "10.1.1.3")
        self.assertEqual(query["sinks"][0]["config"]["socket_host"], "10.1.1.4")
        self.assertEqual(query["workers"][0]["downstream"], ["10.1.1.2:8080"])
        self.assertNotIn("downstream", query["workers"][1])

    def test_dry_run_never_accesses_docker(self):
        with patch.object(deployment, "command", side_effect=AssertionError("Docker accessed")), \
             contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(run_experiment.main(["--dry-run"]), 0)
        self.assertEqual(len(json.loads(output.getvalue())["environment"]["services"]), 2)

    def test_invalid_query_contracts(self):
        for change in (lambda q: q.update(physical=[]),
                       lambda q: q["sinks"][0].update(type="File"),
                       lambda q: q["logical"][0]["schema"][-1].update(type="FLOAT64"),
                       lambda q: q["sinks"][0]["schema"][-1].update(name="missing_ts")):
            query = deepcopy(self.experiment.configs["query"])
            change(query)
            with self.assertRaises(ConfigError):
                resolve_query(query, self.plan, self.experiment.settings)

    def test_status_requires_matching_global_row_and_rejects_errors(self):
        row = {"query_id": "abc", "local_query_id": None, "worker": None, "query_status": "Running", "error": None}
        self.assertEqual(query_status(json.dumps([row]), "abc"), "Running")
        for rows in ([], [row | {"query_id": "other"}], [row | {"error": "broken"}],
                     [row | {"query_status": "Failed"}], [row, row], [row | {"worker": "node1"}]):
            with self.assertRaises(RuntimeError):
                query_status(json.dumps(rows), "abc")

    def prepare(self, directory):
        with patch.object(deployment, "command", return_value=Mock(stdout='[{"Id":"sha256:local"}]')), \
             contextlib.redirect_stdout(io.StringIO()):
            self.experiment.prepare(self.plan, Path(directory))

    def test_artifacts_and_separate_service_resources(self):
        original = (ROOT / "examples/experiment/datagen.yaml").read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            net = Mock()
            with patch.object(deployment, "ensure_tools"):
                deployment.build_network(self.plan, net, object, object, [])
            calls = {call.args[0]: call.kwargs for call in net.addDocker.call_args_list}
            self.assertEqual(calls["datagen"]["dimage"], "secbench/datagen:local")
            self.assertEqual(calls["tracker"]["mem_limit"], "512m")
            self.assertNotIn("port_bindings", calls["datagen"])
            self.assertTrue(any(mount.endswith(":/control:ro") for mount in calls["datagen"]["volumes"]))
            self.assertTrue(any(mount.endswith(":/state:rw") for mount in calls["nebuli"]["volumes"]))
            self.assertEqual(len(net.addLink.call_args_list), 4)
        self.assertEqual((ROOT / "examples/experiment/datagen.yaml").read_bytes(), original)

    def test_success_order_and_release_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            exp = self.experiment
            order = []
            def launch(role, argv):
                order.append(role)
                exp.started.append(role)
            def cli(action, *args):
                order.append(action)
                self.assertFalse((Path(directory) / "control/start").exists())
                states = Path(directory) / "state/nebucli"
                states.mkdir()
                (states / "abc.json").write_text('{"query_id":"abc"}')
                return "abc"
            def wait(description, predicate, timeout):
                order.append(description)
                if description == "datagen completion":
                    self.assertTrue((Path(directory) / "control/start").exists())
            with patch.object(deployment, "inside", return_value=Mock(stdout="1")), \
                 patch.object(exp, "launch", side_effect=launch), patch.object(exp, "cli", side_effect=cli), \
                 patch.object(exp, "wait", side_effect=wait), patch.object(exp, "stop_process"), \
                 patch.object(exp, "exit_code", return_value=0), patch.object(exp, "validate_results"), \
                 contextlib.redirect_stdout(io.StringIO()):
                exp.execute(self.plan, Path(directory))
            self.assertLess(order.index("tracker"), order.index("datagen"))
            self.assertLess(order.index("datagen"), order.index("start"))
            self.assertEqual(exp.summary["status"], "completed")

    def test_failed_query_never_releases_datagen_and_cleans_up(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            exp = self.experiment
            with patch.object(deployment, "inside", return_value=Mock(stdout="1")), \
                 patch.object(exp, "launch"), patch.object(exp, "wait"), \
                 patch.object(exp, "cli", side_effect=RuntimeError("query failed")), \
                 patch.object(exp, "stop_process") as stop, contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "query failed"):
                    exp.execute(self.plan, Path(directory))
            self.assertFalse((Path(directory) / "control/start").exists())
            self.assertEqual([call.args[0] for call in stop.call_args_list], ["datagen", "tracker"])
            self.assertEqual(exp.summary["status"], "failed")

    def test_drain_timeout_is_failure_not_success(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            exp = self.experiment
            actions = []
            def cli(action, *args):
                actions.append(action)
                if action == "start":
                    states = Path(directory) / "state/nebucli"
                    states.mkdir()
                    (states / "abc.json").write_text('{"query_id":"abc"}')
                return ""
            def wait(description, predicate, timeout):
                if "sink EOF" in description:
                    raise RuntimeError("drain timeout")
            with patch.object(deployment, "inside", return_value=Mock(stdout="1")), \
                 patch.object(exp, "launch"), patch.object(exp, "wait", side_effect=wait), \
                 patch.object(exp, "cli", side_effect=cli), patch.object(exp, "stop_process"), \
                 contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(RuntimeError, "drain timeout"):
                    exp.execute(self.plan, Path(directory))
            self.assertEqual(actions, ["start", "stop"])
            self.assertEqual(exp.summary["status"], "failed")

    def test_results_reject_empty_and_invalid_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            path = Path(directory) / "tracker/latency_output.log"
            for value in ("", "-1,1,100,99\n", "2,1,100,103\n"):
                path.write_text(value)
                with self.assertRaises(RuntimeError):
                    self.experiment.validate_results()
            path.write_text("3,1,100,103\n")
            self.experiment.validate_results()
            self.assertEqual(self.experiment.summary["result_rows"], 1)

    def test_sink_drain_waits_for_close_wait_and_unread_data(self):
        with tempfile.TemporaryDirectory() as directory:
            self.prepare(directory)
            for output, drained in (("LISTEN 0 64 10.1.1.4:9000 0.0.0.0:*\n", True),
                                    ("CLOSE-WAIT 4096 0 10.1.1.4:9000 10.1.1.2:30000\n", False),
                                    ("ESTAB 0 0 10.1.1.4:9000 10.1.1.2:30000\n", False),
                                    ("TIME-WAIT 0 0 10.1.1.4:9000 10.1.1.2:30000\n", True)):
                with patch.object(deployment, "inside", return_value=Mock(stdout=output)):
                    self.assertEqual(self.experiment.sink_drained(), drained)


@unittest.skipUnless(os.environ.get("SECBENCH_DATAGEN_BINARY") and os.environ.get("SECBENCH_TRACKER_BINARY"),
                     "Set SECBENCH_DATAGEN_BINARY and SECBENCH_TRACKER_BINARY for local TCP integration")
class ComponentIntegrationTests(unittest.TestCase):
    def connect(self, port, process):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if process.poll() is not None:
                self.fail(f"Component exited early with {process.returncode}")
            try:
                return socket.create_connection(("127.0.0.1", port), timeout=0.2)
            except ConnectionRefusedError:
                time.sleep(0.02)
        self.fail("Component did not listen")

    def test_gate_timer_eof_and_tracker_drain(self):
        processes = []
        with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as stack:
            root = Path(directory)
            ports = []
            for _ in range(2):
                with socket.socket() as probe:
                    probe.bind(("127.0.0.1", 0))
                    ports.append(probe.getsockname()[1])
            _, experiment = load_experiment(ROOT / "config/experiment.yaml")
            datagen = experiment.configs["datagen"]
            datagen.update(host="127.0.0.1", ports=[ports[0]], duration=1)
            datagen["data_source"]["file"] = str(ROOT / "examples/experiment/data.csv")
            tracker = experiment.configs["tracker"]
            tracker["connection"].update(host="127.0.0.1", port=ports[1])
            tracker["processing"].update(spool_file=str(root / "spool.log"), latency_output_file=str(root / "latency.log"),
                                         latency_worker_threads=2)
            for role, cfg in (("datagen", datagen), ("tracker", tracker)):
                (root / f"{role}.yaml").write_text(yaml.safe_dump(cfg))
            try:
                for role in ("tracker", "datagen"):
                    log = stack.enter_context((root / f"{role}.log").open("w"))
                    process = subprocess.Popen([os.environ[f"SECBENCH_{role.upper()}_BINARY"], str(root / f"{role}.yaml")],
                        env=os.environ | {"SECBENCH_START_FILE": str(root / "start")}, cwd=root, stdout=log, stderr=log)
                    processes.append(process)
                tracker_process, datagen_process = processes
                with self.connect(ports[0], datagen_process) as source, self.connect(ports[1], tracker_process) as sink:
                    source.settimeout(1.2)
                    with self.assertRaises(socket.timeout):
                        source.recv(1)
                    self.assertIsNone(datagen_process.poll(), "Duration must not elapse before release")
                    released_ms = time.time_ns() // 1_000_000
                    (root / "start").touch()
                    source.settimeout(5)
                    payload = bytearray()
                    while True:
                        batch = source.recv(65536)
                        if not batch:
                            break
                        payload.extend(batch)
                        sink.sendall(batch)
                self.assertEqual(datagen_process.wait(timeout=5), 0)
                rows = payload.decode().splitlines()
                self.assertGreater(len(rows), 0)
                self.assertTrue(all(int(row.split(",")[-1]) >= released_ms for row in rows))
                # Give the receive thread EOF before requesting spool drain.
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if (root / "spool.log").exists() and len((root / "spool.log").read_text().splitlines()) == len(rows):
                        break
                    time.sleep(0.02)
                tracker_process.terminate()
                self.assertEqual(tracker_process.wait(timeout=5), 0)
                self.assertEqual(len((root / "latency.log").read_text().splitlines()), len(rows))
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()


if __name__ == "__main__":
    unittest.main()
