#!/usr/bin/env python3
"""Deploy NebulaStream workers connected by configuration-defined routed links."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import time
from uuid import uuid4

from nebulastream_environment import ConfigError, load_plan


ROOT = Path(__file__).resolve().parent


def command(args, check=True, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{shlex.join(args)} failed:\n{result.stderr or result.stdout}")
    return result


def inside(host, script, check=True, timeout=30):
    return command(["docker", "exec", f"mn.{host.id}", "bash", "-lc", script], check, timeout)


def published_ports(plan, index):
    cfg = plan.deployment
    if not cfg["publish_ports"]:
        return {}
    # Data sockets bind to routed service IPs and are not reachable through
    # Docker's bridge DNAT. Only publish the wildcard-bound gRPC port.
    return {cfg["grpc_port"]: cfg["host_grpc_port_base"] + index}


def ensure_tools(plan, host):
    tools = "ip tc sysctl timeout nohup ifconfig"
    if host.id == plan.cli.id:
        tools += " ping"
    probe = f"for tool in {tools}; do command -v \"$tool\" >/dev/null || exit 1; done"
    if inside(host, probe, check=False).returncode == 0:
        return
    if not plan.deployment["install_tools"]:
        raise RuntimeError(f"{host.id} is missing network tools. Use an image with iproute2, net-tools, "
                           "procps and iputils-ping, or set deployment.install_tools: true")
    print(f"Installing network tools in {host.id} before network setup", flush=True)
    inside(host, "apt-get update && DEBIAN_FRONTEND=noninteractive "
                 "apt-get install -y --no-install-recommends iproute2 net-tools procps iputils-ping coreutils", timeout=300)
    inside(host, probe)


def preflight(plan):
    if os.geteuid() != 0:
        raise RuntimeError("Deployment requires root; --dry-run can run without root")
    for executable in ("docker", "mnexec", "ip", "tc"):
        if not shutil.which(executable):
            raise RuntimeError(f"Missing {executable}; install the Containernet host dependencies")
    details = json.loads(command(["docker", "info", "--format", "{{json .}}"] ).stdout)
    if not details.get("NCPU"):
        raise RuntimeError("Cannot determine Docker host CPU capacity")
    for host in plan.all_hosts:
        if host.cpu > details["NCPU"]:
            raise RuntimeError(f"{host.id} requests {host.cpu} CPUs; host has {details['NCPU']}")
    names = set(command(["docker", "ps", "-a", "--format", "{{.Names}}"] ).stdout.splitlines())
    conflicts = names.intersection(f"mn.{host.id}" for host in plan.all_hosts)
    if conflicts:
        raise RuntimeError(f"Containers already exist: {', '.join(sorted(conflicts))}. Remove or rename them first")
    interfaces = {entry["ifname"] for entry in json.loads(command(["ip", "-j", "link", "show"]).stdout)}
    planned_interfaces = {interface for entry in plan.links for interface in (entry.left_interface, entry.right_interface)}
    if interfaces.intersection(planned_interfaces):
        raise RuntimeError("Configured names conflict with existing host interfaces")
    networks = json.loads(command(["docker", "network", "inspect", "bridge"]).stdout)
    from ipaddress import IPv4Network
    for entry in networks[0].get("IPAM", {}).get("Config", []):
        if ":" in entry.get("Subnet", ""):
            continue
        bridge_subnet = IPv4Network(entry["Subnet"])
        addresses = [host.ip for host in plan.all_hosts]
        addresses += [address for item in plan.links for address in (item.left_ip, item.right_ip)]
        if any(IPv4Network(address + "/32").overlaps(bridge_subnet) for address in addresses):
            raise RuntimeError("Configured addresses overlap Docker's management bridge")
    images = {plan.deployment["worker_image"], plan.deployment["cli_image"]}
    images.update(settings["image"] for settings in plan.service_settings.values())
    for image in images:
        if command(["docker", "image", "inspect", image], check=False).returncode:
            raise RuntimeError(f"Image {image} is not local; build or pull it before deployment")
    for index in range(len(plan.workers)):
        for port in published_ports(plan, index).values():
            with socket.socket() as probe:
                try:
                    probe.bind(("0.0.0.0", port))
                except OSError as exc:
                    raise RuntimeError(f"Published TCP port {port} is unavailable") from exc


def build_network(plan, net, link_class, interface_class, attempted, host_class=None, run_id=""):
    hosts = {}
    cfg = plan.deployment
    for index, host in enumerate(plan.all_hosts):
        is_cli = host.id == plan.cli.id
        service = plan.service_settings.get(host.id)
        volumes = list(plan.mounts.get(host.id, []))
        if cfg.get("model_dir") and not service:
            volumes.append(f"{cfg['model_dir']}:/tmp/host-model:ro")
        if is_cli and cfg.get("queries_dir"):
            volumes.append(f"{cfg['queries_dir']}:/queries:ro")
        params = {
            "ip": None,
            "service_ip": host.ip,
            "dimage": service["image"] if service else cfg["cli_image" if is_cli else "worker_image"],
            "dcmd": "bash -lc 'trap : TERM INT; while true; do sleep 3600; done'",
            "cpus": host.cpu, "mem_limit": host.memory,
            "environment": {"SECBENCH_RUN_ID": run_id},
            # Docker mounts /proc/sys read-only; set routing before startup.
            "sysctls": {"net.ipv4.ip_forward": "1",
                        "net.ipv4.conf.all.rp_filter": "0",
                        "net.ipv4.conf.default.rp_filter": "0"},
            "volumes": volumes, "publish_all_ports": False,
        }
        if host.storage:
            params["storage_opt"] = {"size": host.storage}
        if host_class is not None:
            params["cls"] = host_class
        if not is_cli and not service:
            params["ports"] = [cfg["grpc_port"], cfg["data_port"]]
            params["port_bindings"] = published_ports(plan, index)
        attempted.append(host)
        hosts[host.id] = net.addDocker(host.id, **params)
        ensure_tools(plan, host)
    for entry in plan.links:
        net.addLink(hosts[entry.left], hosts[entry.right], cls=link_class,
                    intfName1=entry.left_interface, intfName2=entry.right_interface,
                    cls1=interface_class, cls2=interface_class, **entry.tc_params())
    return hosts


def host_commands(plan, host):
    entries = plan.all_hosts
    text = "\n".join(f"{entry.ip} {entry.id}" for entry in entries) + "\n"
    scripts = [
        "command -v ip >/dev/null && command -v timeout >/dev/null",
        'test "$(sysctl -n net.ipv4.ip_forward)" = 1 && '
        'test "$(sysctl -n net.ipv4.conf.all.rp_filter)" = 0 && '
        'test "$(sysctl -n net.ipv4.conf.default.rp_filter)" = 0',
        f"ip link set lo up && ip addr replace {host.ip}/32 dev lo",
    ]
    for entry in plan.links:
        for identifier, address, interface in ((entry.left, entry.left_ip, entry.left_interface),
                                               (entry.right, entry.right_ip, entry.right_interface)):
            if identifier == host.id:
                scripts.append(f"ip link set {interface} up && ip addr flush dev {interface} && "
                               f"ip addr add {address}/30 dev {interface} && "
                               f'test "$(sysctl -n net.ipv4.conf.{interface}.rp_filter)" = 0')
    # Docker seeds /etc/hosts with management IPs. Write in place because
    # /etc/hosts is bind mounted and cannot be replaced using rename.
    names = "|".join(entry.id for entry in entries)
    scripts.append(f"awk '$0 !~ /[[:space:]]({names})([[:space:]]|$)/' /etc/hosts > /tmp/secbench-hosts && "
                   f"printf %s {shlex.quote(text)} >> /tmp/secbench-hosts && cat /tmp/secbench-hosts > /etc/hosts")
    if plan.deployment.get("model_dir") and host.id not in plan.service_settings:
        scripts.append("mkdir -p /model && cp -a /tmp/host-model/. /model/")
    for route in plan.routes:
        if route["host"] == host.id:
            scripts.append(f"ip route replace {route['destination']}/32 via {route['gateway']} "
                           f"dev {route['interface']} src {route['source']}")
    return scripts


def configure_hosts(plan):
    for host in plan.all_hosts:
        inside(host, " && ".join(host_commands(plan, host)))


def start_workers(plan):
    cfg = plan.deployment
    for host in plan.workers:
        inside(host, f"command -v {shlex.quote(cfg['worker_binary'])} >/dev/null")
        argv = [cfg["worker_binary"], *cfg["worker_args"],
                f"--grpc=0.0.0.0:{cfg['grpc_port']}",
                f"--data_address={host.ip}:{cfg['data_port']}"]
        inside(host, f"nohup {shlex.join(argv)} > /tmp/secbench-worker.log 2>&1 < /dev/null & "
                     "echo $! > /tmp/secbench-worker.pid")


def wait_ready(plan):
    deadline = time.monotonic() + plan.deployment["startup_timeout"]
    pending = {host.id: host for host in plan.workers}
    while pending and time.monotonic() < deadline:
        for identifier, host in list(pending.items()):
            if time.monotonic() >= deadline:
                break
            alive = inside(host, 'kill -0 "$(cat /tmp/secbench-worker.pid)"', check=False)
            if alive.returncode:
                logs = inside(host, "tail -n 30 /tmp/secbench-worker.log", check=False).stdout
                raise RuntimeError(f"Worker {identifier} exited during startup:\n{logs}")
            # Check the routed service address, not Docker's management bridge.
            probe = f"exec 3<>/dev/tcp/{host.ip}/{plan.deployment['grpc_port']}"
            ready = inside(plan.cli, f"timeout 1 bash -c {shlex.quote(probe)}", check=False)
            if ready.returncode == 0:
                del pending[identifier]
        if pending:
            time.sleep(0.25)
    if pending:
        raise RuntimeError(f"Workers not reachable from nebuli before timeout: {', '.join(pending)}")


def collect_logs(plan, output_dir):
    for host in plan.workers:
        try:
            result = inside(host, "cat /tmp/secbench-worker.log", check=False, timeout=5)
            if not result.returncode:
                (output_dir / f"{host.id}.log").write_text(result.stdout)
        except (OSError, subprocess.TimeoutExpired):
            pass


def remove_owned_container(host, run_id):
    result = command(["docker", "inspect", "--type", "container", f"mn.{host.id}"], check=False)
    if result.returncode:
        return
    container = json.loads(result.stdout)[0]
    if f"SECBENCH_RUN_ID={run_id}" in (container.get("Config", {}).get("Env") or []):
        command(["docker", "rm", "-f", container["Id"]], check=False)


def run(plan, output_dir, smoke_test=False, headless=False, lifecycle=None):
    preflight(plan)
    # Previewing configurations needs only PyYAML, not privileged dependencies.
    from mininet.net import Containernet
    from mininet.node import Docker
    from mininet.link import TCIntf, TCLink
    from mininet.cli import CLI
    from mininet.log import setLogLevel

    class RoutedDocker(Docker):
        def IP(self, intf=None):
            if intf is None:
                return self.params["service_ip"]
            return super().IP(intf)

    class BenchmarkIntf(TCIntf):
        bwParamMax = 1000_000

        def config(self, **params):
            result = super().config(**params)
            for output in result.get("tcoutputs", []):
                if output.strip() and "No such file or directory" not in output and "Warning:" not in output:
                    raise RuntimeError(f"Traffic shaping failed on {self.name}: {output!r}")
            return result

    setLogLevel("info")
    output_dir.mkdir(parents=True, exist_ok=False)
    if lifecycle is not None:
        lifecycle.prepare(plan, output_dir)
    manifest = plan.as_dict()
    run_id = uuid4().hex
    manifest["run_id"] = run_id
    manifest["endpoints"] = [
        {"id": host.id, "grpc": f"{host.ip}:{plan.deployment['grpc_port']}",
         "data": f"{host.ip}:{plan.deployment['data_port']}",
         "published_ports": published_ports(plan, index)}
        for index, host in enumerate(plan.workers)
    ]
    (output_dir / "deployment.json").write_text(json.dumps(manifest, indent=2) + "\n")
    net, attempted = None, []
    try:
        net = Containernet(controller=None, ipBase=plan.subnet)
        build_network(plan, net, TCLink, BenchmarkIntf, attempted, RoutedDocker, run_id)
        net.start()
        configure_hosts(plan)
        start_workers(plan)
        wait_ready(plan)
        print(f"Ready: {len(plan.workers)} workers, {len(plan.links)} routed links", flush=True)
        print(f"Deployment record and worker logs: {output_dir}", flush=True)
        if lifecycle is not None:
            lifecycle.execute(plan, output_dir)
        elif smoke_test:
            for host in plan.workers:
                inside(plan.cli, f"ping -I {plan.cli.ip} -c 1 -W 5 {host.ip}")
        elif headless:
            while True:
                time.sleep(1)
        else:
            CLI(net)
    finally:
        try:
            collect_logs(plan, output_dir)
        finally:
            try:
                if net is not None:
                    net.stop()
            finally:
                # addDocker may fail after creation but before net.hosts is updated.
                for host in attempted:
                    remove_owned_container(host, run_id)


def interrupted(*_):
    raise KeyboardInterrupt


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/nebulastream.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print expanded JSON without creating resources")
    parser.add_argument("--output-dir", type=Path, help="New directory for deployment record and worker logs")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--smoke-test", action="store_true", help="Start, check readiness and ping, then clean up")
    modes.add_argument("--headless", action="store_true", help="Remain running until SIGINT/SIGTERM instead of opening the CLI")
    args = parser.parse_args(argv)
    try:
        plan = load_plan(args.config)
        if args.dry_run:
            print(json.dumps(plan.as_dict(), indent=2))
            return 0
        output = args.output_dir or ROOT / "runs" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{os.getpid()}")
        previous = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, interrupted)
        try:
            run(plan, output.resolve(), args.smoke_test, args.headless)
        finally:
            signal.signal(signal.SIGTERM, previous)
        return 0
    except KeyboardInterrupt:
        return 130
    except (ConfigError, RuntimeError, OSError, ImportError, subprocess.SubprocessError) as exc:
        print(f"Deployment failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
