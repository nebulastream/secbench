"""Optional real CLI/worker test on Docker bridge, without Mininet or root.

This verifies the application protocol, not routed links or traffic shaping.
"""

import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from uuid import uuid4

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from nebulastream_experiment import load_experiment, query_status


@unittest.skipUnless(os.environ.get("SECBENCH_LIVE_CLI") == "1", "Set SECBENCH_LIVE_CLI=1 for Docker bridge integration")
class NebulaStreamIntegrationTests(unittest.TestCase):
    def command(self, *args, timeout=60):
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            self.fail(f"{args} failed:\n{result.stdout}\n{result.stderr}")
        return result.stdout

    def test_two_worker_query_gate_and_natural_completion(self):
        for key in ("SECBENCH_DATAGEN_BINARY", "SECBENCH_TRACKER_BINARY"):
            if key not in os.environ:
                self.skipTest(f"{key} is required")
        containers, processes = [], []
        token = uuid4().hex[:10]
        worker_image = os.environ.get("SECBENCH_WORKER_IMAGE", "nebulastream/worker:tt-tcp-sink")
        cli_image = os.environ.get("SECBENCH_CLI_IMAGE", "nebulastream/nes-cli:tt-tcp-sink")
        with tempfile.TemporaryDirectory(prefix="secbench-cli-") as directory, contextlib.ExitStack() as stack:
            root = Path(directory)
            try:
                bridge = json.loads(self.command("docker", "network", "inspect", "bridge"))[0]
                gateway = bridge["IPAM"]["Config"][0]["Gateway"]
                plan, experiment = load_experiment(ROOT / "config/experiment.yaml")
                query = experiment.configs["query"]
                addresses = []
                for index in range(2):
                    identifier = self.command("docker", "run", "-d", "--name", f"secbench-check-{token}-{index}",
                        "--memory", "1g", "--cpus", "1", "--entrypoint", "bash", worker_image,
                        "-lc", "sleep infinity").strip()
                    containers.append(identifier)
                    address = json.loads(self.command("docker", "inspect", identifier))[0]["NetworkSettings"]["Networks"]["bridge"]["IPAddress"]
                    addresses.append(address)
                    self.command("docker", "exec", "-d", identifier, "bash", "-lc",
                        f"nes-single-node-worker --grpc=0.0.0.0:8080 --data_address={address}:9090 "
                        "--worker.default_query_execution.operator_buffer_size=4096 "
                        "--worker.number_of_buffers_in_global_buffer_manager=1024 > /tmp/worker.log 2>&1")
                    deadline = time.monotonic() + 30
                    while True:
                        try:
                            with socket.create_connection((address, 8080), timeout=0.2):
                                break
                        except OSError:
                            if time.monotonic() > deadline:
                                self.fail(self.command("docker", "exec", identifier, "cat", "/tmp/worker.log"))
                            time.sleep(0.1)
                ports = []
                for _ in range(2):
                    with socket.socket() as probe:
                        probe.bind((gateway, 0))
                        ports.append(probe.getsockname()[1])
                query["workers"] = [
                    {"host": f"{addresses[0]}:8080", "data_address": f"{addresses[0]}:9090",
                     "max_operators": 10000, "downstream": [f"{addresses[1]}:8080"]},
                    {"host": f"{addresses[1]}:8080", "data_address": f"{addresses[1]}:9090", "max_operators": 10000},
                ]
                query["physical"][0]["host"] = f"{addresses[0]}:8080"
                query["physical"][0]["source_config"].update(socket_host=gateway, socket_port=str(ports[0]))
                query["sinks"][0]["host"] = f"{addresses[1]}:8080"
                query["sinks"][0]["config"].update(socket_host=gateway, socket_port=str(ports[1]))
                (root / "query.yaml").write_text(yaml.safe_dump(query))
                datagen = experiment.configs["datagen"]
                datagen.update(host=gateway, ports=[ports[0]], duration=2)
                datagen["data_source"]["file"] = str(ROOT / "examples/experiment/data.csv")
                tracker = experiment.configs["tracker"]
                tracker["connection"].update(host=gateway, port=ports[1])
                tracker["processing"].update(spool_file=str(root / "spool.log"),
                    latency_output_file=str(root / "latency.log"), latency_worker_threads=2)
                for role, config in (("tracker", tracker), ("datagen", datagen)):
                    path = root / f"{role}.yaml"
                    path.write_text(yaml.safe_dump(config))
                    log = stack.enter_context((root / f"{role}.log").open("w"))
                    process = subprocess.Popen([os.environ[f"SECBENCH_{role.upper()}_BINARY"], str(path)],
                        cwd=root, env=os.environ | {"SECBENCH_START_FILE": str(root / "start")}, stdout=log, stderr=log)
                    processes.append(process)
                def sockets(port, state):
                    return bool(self.command("ss", "-Hnt", "state", state, f"( sport = :{port} )").strip())
                deadline = time.monotonic() + 10
                while not all(sockets(port, "listening") for port in ports):
                    self.assertTrue(all(p.poll() is None for p in processes))
                    self.assertLess(time.monotonic(), deadline)
                    time.sleep(0.1)
                def cli(action, *args):
                    return self.command("docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}",
                        "-v", f"{root}:/experiment:rw", "-w", "/experiment", "-e", "XDG_STATE_HOME=/experiment/state",
                        "--entrypoint", "nes-cli", cli_image, "-t", "/experiment/query.yaml", action, *args)
                def sink_drained():
                    output = self.command("ss", "-Hnt", "state", "all", f"( sport = :{ports[1]} )")
                    return all(row.split()[0] in ("LISTEN", "TIME-WAIT", "CLOSED") for row in output.splitlines())
                cli("start")
                states = list((root / "state/nebucli").glob("*.json"))
                self.assertEqual(len(states), 1)
                query_id = json.loads(states[0].read_text())["query_id"]
                deadline = time.monotonic() + 30
                while query_status(cli("status", query_id), query_id) != "Running" or not all(
                        sockets(port, "established") for port in ports):
                    self.assertLess(time.monotonic(), deadline, "Query endpoints did not become ready before release")
                    time.sleep(0.1)
                self.assertEqual((root / "spool.log").read_text(), "")
                (root / "start").touch()
                self.assertEqual(processes[1].wait(timeout=15), 0)
                deadline = time.monotonic() + 30
                while query_status(cli("status", query_id), query_id) != "Stopped" or not sink_drained():
                    self.assertLess(time.monotonic(), deadline, "Query did not complete naturally")
                    time.sleep(0.1)
                processes[0].terminate()
                self.assertEqual(processes[0].wait(timeout=5), 0)
                rows = (root / "latency.log").read_text().splitlines()
                self.assertGreater(len(rows), 0)
                for row in rows:
                    latency, identifier, sent, received = map(int, row.split(","))
                    self.assertIn(identifier, (1, 2, 3))
                    self.assertEqual(latency, received - sent)
            finally:
                for process in processes:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                for identifier in containers:
                    subprocess.run(["docker", "rm", "-f", identifier], capture_output=True, timeout=15)


if __name__ == "__main__":
    unittest.main()
