"""Root-free tests for configuration, routing and deployment lifecycle."""

import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import deploy_nebulastream as launcher
from nebulastream_environment import ConfigError, load_plan, make_plan


def configuration(topology="line", count=3):
    return {"hardware": [{"id": "node", "count": count, "cpu": 0.5, "memory": "1g"}],
            "network": {"topology": topology, "default_link": {"latency": 15, "bandwidth": 200}}}


class ConfigurationTests(unittest.TestCase):
    def test_samples_and_paths(self):
        small = load_plan(ROOT / "config/nebulastream.yaml")
        self.assertEqual(len(small.workers), 3)
        self.assertEqual(small.deployment["queries_dir"], str(ROOT / "que"))
        self.assertEqual(small.workers[2].cpu, 0.5)
        large = load_plan(ROOT / "config/nebulastream-scaled.yaml")
        self.assertEqual(len(large.workers), 64)
        self.assertEqual(len(large.links), 64)
        self.assertEqual(len(large.routes), 65 * 64)
        self.assertEqual([host.id for host in large.workers][::63], ["node1", "node64"])

    def test_line_multi_hop_routes_and_reverse_path(self):
        plan = make_plan(configuration(), ROOT)
        routes = {(route["host"], route["destination"]): route for route in plan.routes}
        self.assertEqual(routes["node1", "10.1.1.3"]["gateway"], "10.200.0.2")
        self.assertEqual(routes["node3", "10.1.1.1"]["gateway"], "10.200.0.5")
        self.assertEqual(routes["node1", "10.1.1.3"]["source"], "10.1.1.1")
        self.assertEqual(routes["nebuli", "10.1.1.3"]["gateway"], "10.200.0.10")
        self.assertEqual(len(plan.routes), 12)
        interfaces = [interface for item in plan.links for interface in (item.left_interface, item.right_interface)]
        self.assertEqual(len(interfaces), len(set(interfaces)))

    def test_ring_uses_shortest_path_and_routes_never_loop(self):
        for topology in ("ring", "line", "tree", "star"):
            with self.subTest(topology=topology):
                plan = make_plan(configuration(topology, 8), ROOT)
                hosts = [*plan.workers, plan.cli]
                ip_to_host = {host.ip: host.id for host in hosts}
                peers = {}
                for item in plan.links:
                    peers[item.left, item.right_ip] = item.right
                    peers[item.right, item.left_ip] = item.left
                routes = {(r["host"], r["destination"]): peers[r["host"], r["gateway"]] for r in plan.routes}
                for source in hosts:
                    for destination in hosts:
                        current, visited = source.id, set()
                        while current != destination.id:
                            self.assertNotIn(current, visited)
                            visited.add(current)
                            current = routes[current, destination.ip]
                        self.assertEqual(current, ip_to_host[destination.ip])
                if topology == "ring":
                    self.assertEqual(routes["node1", "10.1.1.8"], "node8")

    def test_custom_links_keep_properties_and_cli_attachment(self):
        cfg = configuration("custom")
        cfg["network"].update(cli_node="node3", links=[
            {"link": "node1:node2", "latency": 5, "bandwidth": 10},
            {"link": "node2:node3", "loss": 2}])
        plan = make_plan(cfg, ROOT)
        self.assertEqual(plan.links[0].tc_params(), {"delay": "5ms", "bw": 10, "loss": 0})
        self.assertEqual(plan.links[1].latency, 15)
        self.assertEqual(plan.links[1].loss, 2)
        self.assertEqual(plan.links[-1].right, "node3")

    def test_single_worker(self):
        plan = make_plan(configuration(count=1), ROOT)
        self.assertEqual(len(plan.links), 1)
        self.assertEqual(len(plan.routes), 2)

    def test_invalid_configurations(self):
        mutations = [
            lambda c: c.update(unknown=True),
            lambda c: c.update(hardware=[]),
            lambda c: c["hardware"][0].update(cpu=0),
            lambda c: c["hardware"][0].update(cpu=True),
            lambda c: c["hardware"][0].update(cpu=float("nan")),
            lambda c: c["hardware"][0].update(memory="bad"),
            lambda c: c["hardware"][0].update(count=1.5),
            lambda c: c["hardware"][0].update(id="bad;cmd"),
            lambda c: c["hardware"].append({"id": "node1", "cpu": 1, "memory": "1g"}),
            lambda c: c["hardware"].append({"id": "nebuli", "cpu": 1, "memory": "1g"}),
            lambda c: c["network"].update(topology="unknown"),
            lambda c: c["network"].update(subnet="not-a-subnet"),
            lambda c: c["network"].update(subnet="10.1.1.0/30"),
            lambda c: c["network"].update(link_subnet="10.1.1.0/24"),
            lambda c: c["network"].update(link_subnet="10.200.0.0/30"),
            lambda c: c["network"].update(cli_node="absent"),
            lambda c: c["network"].update(default_link={"latency": -1}),
            lambda c: c["network"].update(default_link={"bandwidth": 0}),
            lambda c: c["network"].update(default_link={"loss": 101}),
            lambda c: c["network"].update(links=[]),
            lambda c: c.update(deployment={"publish_ports": "true"}),
            lambda c: c.update(deployment={"install_tools": "true"}),
            lambda c: c.update(deployment={"publish_ports": True, "host_grpc_port_base": 65535}),
            lambda c: c.update(deployment={"queries_dir": "absent-directory"}),
            lambda c: c.update(deployment={"worker_args": "--flag"}),
            lambda c: c.update(deployment={"worker_args": ["--grpc=0.0.0.0:8"]}),
        ]
        for index, mutate in enumerate(mutations):
            with self.subTest(index=index):
                cfg = configuration()
                mutate(cfg)
                with self.assertRaises(ConfigError):
                    make_plan(cfg, ROOT)

    def test_invalid_custom_graphs(self):
        for pairs in (["node1:missing"], ["node1:node1"], ["node1:node2", "node2:node1"],
                      ["node1:node2"], ["nebuli:node1"], ["node1"]):
            with self.subTest(pairs=pairs):
                cfg = configuration("custom")
                cfg["network"]["links"] = [{"link": pair} for pair in pairs]
                with self.assertRaises(ConfigError):
                    make_plan(cfg, ROOT)

    def test_dry_run_without_docker(self):
        with patch.object(launcher, "preflight", side_effect=AssertionError("must not access Docker")), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(launcher.main(["--dry-run"]), 0)
        self.assertEqual(len(json.loads(output.getvalue())["workers"]), 3)

    def test_invalid_yaml_fails_before_deployment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "broken.yaml"
            path.write_text("hardware: [\n")
            with self.assertRaises(ConfigError):
                load_plan(path)


class DeploymentTests(unittest.TestCase):
    def setUp(self):
        self.plan = make_plan(configuration(), ROOT)

    def test_resource_and_port_mapping_and_actual_links(self):
        cfg = configuration()
        cfg["deployment"] = {"publish_ports": True}
        cfg["hardware"][0]["storage"] = "16g"
        plan = make_plan(cfg, ROOT)
        net = Mock()
        net.addDocker.side_effect = lambda identifier, **_: identifier
        attempted = []
        with patch.object(launcher, "ensure_tools"):
            launcher.build_network(plan, net, object, object, attempted)
        kwargs = net.addDocker.call_args_list[0].kwargs
        self.assertEqual(kwargs["cpus"], 0.5)
        self.assertEqual(kwargs["mem_limit"], "1g")
        self.assertEqual(kwargs["storage_opt"], {"size": "16g"})
        self.assertEqual(kwargs["port_bindings"], {8080: 18080})
        self.assertFalse(kwargs["publish_all_ports"])
        for call in net.addDocker.call_args_list:
            self.assertEqual(call.kwargs["sysctls"], {
                "net.ipv4.ip_forward": "1",
                "net.ipv4.conf.all.rp_filter": "0",
                "net.ipv4.conf.default.rp_filter": "0",
            })
        self.assertEqual(net.addLink.call_args_list[1].args, ("node2", "node3"))
        self.assertEqual(net.addLink.call_args_list[1].kwargs["bw"], 200)
        self.assertEqual(net.addLink.call_args_list[1].kwargs["intfName1"], "node2-eth1")
        net.addSwitch.assert_not_called()

    def test_host_commands_install_loopback_forwarding_and_routes(self):
        scripts = launcher.host_commands(self.plan, self.plan.workers[1])
        text = "\n".join(scripts)
        self.assertIn('test "$(sysctl -n net.ipv4.ip_forward)" = 1', text)
        self.assertIn('test "$(sysctl -n net.ipv4.conf.node2-eth1.rp_filter)" = 0', text)
        self.assertNotIn("sysctl -w", text)
        self.assertIn("ip addr replace 10.1.1.2/32 dev lo", text)
        self.assertIn("ip addr add 10.200.0.5/30 dev node2-eth1", text)
        self.assertIn("ip route replace 10.1.1.3/32 via 10.200.0.6 dev node2-eth1 src 10.1.1.2", text)

    def test_missing_tools_install_only_when_enabled(self):
        with patch.object(launcher, "inside", return_value=subprocess.CompletedProcess([], 1, "", "")):
            with self.assertRaisesRegex(RuntimeError, "install_tools"):
                launcher.ensure_tools(self.plan, self.plan.cli)
        cfg = configuration()
        cfg["deployment"] = {"install_tools": True}
        plan = make_plan(cfg, ROOT)
        results = [subprocess.CompletedProcess([], code, "", "") for code in (1, 0, 0)]
        with patch.object(launcher, "inside", side_effect=results) as inside, contextlib.redirect_stdout(io.StringIO()):
            launcher.ensure_tools(plan, plan.cli)
        self.assertIn("apt-get install", inside.call_args_list[1].args[1])
        self.assertEqual(inside.call_args_list[1].kwargs["timeout"], 300)

    def test_worker_failure_reports_log(self):
        def response(host, script, **kwargs):
            if "kill -0" in script:
                return subprocess.CompletedProcess([], 1, "", "")
            return subprocess.CompletedProcess([], 0, "worker failed to bind", "")
        with patch.object(launcher, "inside", side_effect=response):
            with self.assertRaisesRegex(RuntimeError, "worker failed to bind"):
                launcher.wait_ready(self.plan)

    def test_readiness_uses_routed_addresses(self):
        with patch.object(launcher, "inside", return_value=subprocess.CompletedProcess([], 0, "", "")) as inside:
            launcher.wait_ready(self.plan)
        probes = [call for call in inside.call_args_list if "/dev/tcp/" in call.args[1]]
        self.assertEqual(len(probes), 3)
        self.assertTrue(all(call.args[0].id == "nebuli" for call in probes))
        self.assertIn("/dev/tcp/10.1.1.3/8080", probes[-1].args[1])

    def test_readiness_timeout(self):
        with patch.object(launcher.time, "monotonic", side_effect=[0, 100]):
            with self.assertRaisesRegex(RuntimeError, "not reachable"):
                launcher.wait_ready(self.plan)

    def test_cleanup_after_partial_creation_and_configuration_failure(self):
        for fail_at in ("creation", "configuration", "none"):
            with self.subTest(fail_at=fail_at), tempfile.TemporaryDirectory() as directory:
                net = Mock()
                net.addDocker.side_effect = RuntimeError("creation failed") if fail_at == "creation" else None
                modules = {"mininet." + key: types.ModuleType("mininet." + key) for key in ("net", "node", "link", "cli", "log")}
                modules["mininet.net"].Containernet = Mock(return_value=net)
                modules["mininet.node"].Docker = type("Docker", (), {})
                modules["mininet.link"].TCIntf = type("TCIntf", (), {})
                modules["mininet.link"].TCLink = object
                modules["mininet.cli"].CLI = Mock()
                modules["mininet.log"].setLogLevel = Mock()
                with patch.dict(sys.modules, modules), patch.object(launcher, "preflight"), \
                     patch.object(launcher, "inside", return_value=subprocess.CompletedProcess([], 0, "", "")), \
                     patch.object(launcher, "configure_hosts", side_effect=RuntimeError("configuration failed") if fail_at == "configuration" else None), \
                     patch.object(launcher, "start_workers"), patch.object(launcher, "wait_ready"), \
                     patch.object(launcher, "remove_owned_container") as remove, contextlib.redirect_stdout(io.StringIO()):
                    if fail_at == "none":
                        launcher.run(self.plan, Path(directory) / "run", smoke_test=True)
                    else:
                        with self.assertRaisesRegex(RuntimeError, "failed"):
                            launcher.run(self.plan, Path(directory) / "run", smoke_test=True)
                net.stop.assert_called_once()
                removed = [call.args[0].id for call in remove.call_args_list]
                self.assertEqual(removed, ["node1"] if fail_at == "creation" else ["node1", "node2", "node3", "nebuli"])

    def test_fallback_cleanup_checks_ownership_and_removes_by_id(self):
        for owner in ("this-run", "different-run"):
            container = {"Id": "container-id", "Config": {"Env": [f"SECBENCH_RUN_ID={owner}"]}}
            response = subprocess.CompletedProcess([], 0, json.dumps([container]), "")
            with patch.object(launcher, "command", return_value=response) as command:
                launcher.remove_owned_container(self.plan.workers[0], "this-run")
            if owner == "this-run":
                self.assertEqual(command.call_args_list[-1].args[0], ["docker", "rm", "-f", "container-id"])
            else:
                self.assertEqual(command.call_count, 1)

    def test_preflight_preserves_existing_containers(self):
        responses = [subprocess.CompletedProcess([], 0, '{"NCPU": 8}', ""),
                     subprocess.CompletedProcess([], 0, 'mn.node1\n', "")]
        with patch.object(launcher.os, "geteuid", return_value=0), \
             patch.object(launcher.shutil, "which", return_value="/bin/tool"), \
             patch.object(launcher, "command", side_effect=responses) as command:
            with self.assertRaisesRegex(RuntimeError, "already exist"):
                launcher.preflight(self.plan)
        self.assertEqual(command.call_count, 2)


class DockerResourceTests(unittest.TestCase):
    def test_container_shell_disables_readline_control_sequences(self):
        try:
            from mininet.node import Docker
        except ImportError as exc:
            self.skipTest(f"Containernet dependencies unavailable: {exc}")
        host = Docker.__new__(Docker)
        host.name, host.dnameprefix, host.shell = "node1", "mn", None
        host._popen = Mock(side_effect=RuntimeError("shell launch intercepted"))
        with patch("mininet.node.pty.openpty", return_value=(10, 11)):
            with self.assertRaisesRegex(RuntimeError, "shell launch intercepted"):
                host.startShell()
        command = host._popen.call_args.args[0]
        self.assertIn("--noediting", command)
        self.assertIn("PS1=" + chr(127), command)

    def test_fractional_cpus_pass_docker_sdk_validation(self):
        try:
            from mininet.node import Docker, Host
            from docker.types import HostConfig
        except ImportError as exc:
            self.skipTest(f"Containernet dependencies unavailable: {exc}")
        client = Mock()
        client.api.create_host_config.side_effect = lambda **kwargs: HostConfig(version="1.43", **kwargs)
        client.api.inspect_container.return_value = {"Id": "fake"}
        def init_host(host, identifier, **kwargs):
            host.name = identifier
            host.params = kwargs
        with patch("mininet.node.docker.from_env", return_value=client), \
             patch.object(Docker, "_check_image_exists"), patch.object(Docker, "update_resources"), \
             patch.object(Host, "__init__", init_host):
            host = Docker("node1", dimage="example:local", cpus=0.5, mem_limit="1g")
        self.assertEqual(host.nano_cpus, 500_000_000)
        self.assertIsInstance(host.nano_cpus, int)
        self.assertEqual(client.api.create_container.call_args.kwargs["host_config"]["NanoCpus"], 500_000_000)


if __name__ == "__main__":
    unittest.main()
