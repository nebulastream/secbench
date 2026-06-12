#!/usr/bin/env python3

from cloudedgebench_nes_simple import main as _simple_main

if __name__ == "__main__":
    _simple_main()
    raise SystemExit

import argparse
import os
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from ipaddress import IPv4Network
from pathlib import Path
import re

import yaml
from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel

os.environ["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:" + os.environ.get("PATH", "")

setLogLevel("info")

DEFAULT_CONFIG_PATH = Path("config/config.yaml")
DEFAULT_IMAGE = "nebulastream/worker:distributed-poc-case-when"
NEBULI_IMAGE = "nebulastream/nes-cli:distributed-poc-case-when"
QUERIES_DIR = os.environ.get("NES_QUERIES_DIR", os.path.join(os.getcwd(), "que"))
WORKER_BIN = os.environ.get("NES_WORKER_BIN", "nes-single-node-worker")
KEEPALIVE_CMD = "bash -lc 'trap : TERM INT; while true; do sleep 3600; done'"
LINK_SUBNET_POOL = IPv4Network("10.200.0.0/16")
MANAGEMENT_SUBNET = IPv4Network("10.1.1.0/24")
LINK_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+:[A-Za-z0-9_-]+$")
HOST_CPU_LIMIT = os.cpu_count() or 1
NEBULI_NAME = "nebuli"
NEBULI_MANAGEMENT_IP = str(MANAGEMENT_SUBNET.network_address + 254)
WORKER_GRPC_PORT = 8080
WORKER_DATA_PORT = 9090
CONTAINER_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
WORKER_GRPC_PORT_HEX = f"{WORKER_GRPC_PORT:04X}"


@dataclass
class NodeSpec:
    name: str
    cpu: int
    memory: str
    storage: str
    management_ip: str


@dataclass
class LinkSpec:
    left: str
    right: str
    latency_ms: float
    bandwidth_mbps: float


@dataclass
class LinkRuntime:
    spec: LinkSpec
    intf1: object
    intf2: object
    left_ip: str
    right_ip: str
    left_intf: str
    right_intf: str


@dataclass
class ExtraHostSpec:
    name: str
    management_ip: str


def remove_existing_containers(container_names):
    for name in container_names:
        deployed_name = f"mn.{name}"
        result = subprocess.run(
            ["docker", "rm", "-f", deployed_name],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            info(f"*** Removed existing container {deployed_name}\n")


def nebuli_volumes():
    if os.path.isdir(QUERIES_DIR):
        return [f"{os.path.abspath(QUERIES_DIR)}:/queries:ro"]
    info(f"*** Warning: queries directory not found at {QUERIES_DIR}; starting nebuli without /queries mount\n")
    return []


def strip_comments(raw_line):
    return raw_line.split("#", 1)[0].rstrip()


def parse_scalar(value):
    value = value.strip().strip('"').strip("'")
    if not value:
        return value
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def parse_config(raw_text):
    try:
        parsed = yaml.safe_load(raw_text)
    except yaml.YAMLError:
        parsed = None

    if isinstance(parsed, dict) and parsed.get("heterogeneity") and parsed.get("network"):
        return parsed

    heterogeneity = []
    links = []
    section = None
    current_node = None
    current_link = None
    network = {}

    for raw_line in raw_text.splitlines():
        line = strip_comments(raw_line)
        if not line.strip():
            continue
        stripped = line.strip()

        if stripped == "heterogeneity:":
            section = "heterogeneity"
            current_node = None
            continue
        if stripped == "network:":
            section = "network"
            current_link = None
            continue

        if section == "heterogeneity":
            if stripped.startswith("- "):
                current_node = {"name": stripped[2:].strip()}
                heterogeneity.append(current_node)
                continue
            if ":" in stripped and current_node is not None:
                key, value = stripped.split(":", 1)
                current_node[key.strip()] = parse_scalar(value)
                continue

        if section == "network":
            if stripped.startswith("topology:"):
                _, value = stripped.split(":", 1)
                network["topology"] = str(parse_scalar(value))
                continue
            if stripped.startswith("- "):
                payload = stripped[2:].strip()
                if LINK_NAME_PATTERN.match(payload):
                    left, right = payload.split(":", 1)
                    current_link = {"left": left, "right": right}
                    links.append(current_link)
                    continue
                if ":" in payload and current_link is not None:
                    key, value = payload.split(":", 1)
                    current_link[key.strip()] = parse_scalar(value)
                    continue
            if ":" in stripped and current_link is not None:
                key, value = stripped.split(":", 1)
                current_link[key.strip()] = parse_scalar(value)

    if heterogeneity and network:
        network["links"] = links
        return {"heterogeneity": heterogeneity, "network": network}

    raise ValueError(f"Could not parse config file content from {DEFAULT_CONFIG_PATH}")


def load_config(config_path):
    config = parse_config(config_path.read_text(encoding="utf-8"))
    nodes = []
    for index, entry in enumerate(config["heterogeneity"], start=1):
        nodes.append(
            NodeSpec(
                name=entry["name"],
                cpu=int(entry["cpu"]),
                memory=str(entry["memory"]),
                storage=str(entry["storage"]),
                management_ip=str(MANAGEMENT_SUBNET.network_address + index),
            )
        )

    if config["network"].get("topology") != "custom":
        raise ValueError("Only network.topology=custom is supported right now.")

    node_names = {node.name for node in nodes}
    links = []
    for entry in config["network"].get("links", []):
        if entry["left"] not in node_names or entry["right"] not in node_names:
            raise ValueError(f"Unknown node in link: {entry}")
        links.append(
            LinkSpec(
                left=entry["left"],
                right=entry["right"],
                latency_ms=float(entry["latency"]),
                bandwidth_mbps=float(entry["bandwidth"]),
            )
        )

    if not links:
        raise ValueError("The custom topology must define at least one link.")

    ensure_connected(nodes, links)
    return nodes, links


def ensure_connected(nodes, links):
    adjacency = {node.name: set() for node in nodes}
    for link in links:
        adjacency[link.left].add(link.right)
        adjacency[link.right].add(link.left)

    queue = deque([nodes[0].name])
    visited = set()
    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)
        queue.extend(adjacency[current] - visited)

    if len(visited) != len(nodes):
        missing = sorted({node.name for node in nodes} - visited)
        raise ValueError(f"The custom topology graph must be connected. Missing: {missing}")


def configure_hosts_file(hosts, nodes, extra_hosts=None):
    info("*** Configuring /etc/hosts entries\n")
    all_hosts = list(nodes) + list(extra_hosts or [])
    for host in hosts.values():
        for node in all_hosts:
            host.cmd(
                "sh -lc \""
                "grep -vw '%s' /etc/hosts > /tmp/hosts.cebench || true; "
                "cat /tmp/hosts.cebench > /etc/hosts && "
                "echo '%s %s' >> /etc/hosts"
                "\""
                % (
                    node.name,
                    node.management_ip,
                    node.name,
                )
            )


def replace_host_entry(host, name, ip):
    host.cmd(
        "sh -lc \""
        "grep -vw '%s' /etc/hosts > /tmp/hosts.cebench || true; "
        "cat /tmp/hosts.cebench > /etc/hosts && "
        "echo '%s %s' >> /etc/hosts"
        "\""
        % (name, ip, name)
    )


def configure_nebuli_direct_hosts(hosts, nebuli_runtimes):
    info("*** Configuring nebuli direct host entries\n")
    for runtime in nebuli_runtimes:
        node_name = runtime.spec.right
        replace_host_entry(hosts[NEBULI_NAME], node_name, runtime.right_ip)
        replace_host_entry(hosts[node_name], NEBULI_NAME, runtime.left_ip)


def install_debug_tools(hosts):
    info("*** Installing required runtime/network tools inside nodes\n")
    for name, host in hosts.items():
        info(f"***   {name}\n")
        output = host.cmd(
            "bash -lc 'export PATH="
            + CONTAINER_PATH
            + "; apt-get update && "
            + "DEBIAN_FRONTEND=noninteractive apt-get install -y "
            + "iputils-ping iproute2 net-tools'"
        )
        if "Unable to locate package" in output or "Temporary failure" in output or "Could not resolve" in output:
            info(f"*** Warning: failed to install tools in {name}:\n{output}\n")


def require_commands(hosts, host_names, commands):
    info("*** Verifying required commands inside containers\n")
    for host_name in host_names:
        host = hosts[host_name]
        for command in commands:
            result = host.cmd(
                "bash -lc 'export PATH="
                + CONTAINER_PATH
                + f"; command -v {command} >/dev/null 2>&1; echo $?'"
            ).strip().splitlines()[-1]
            if result != "0":
                raise RuntimeError(f"Required command '{command}' is missing in container {host_name}")


def has_command(host, command):
    result = host.cmd(
        "bash -lc 'export PATH="
        + CONTAINER_PATH
        + f"; command -v {command} >/dev/null 2>&1; echo $?'"
    ).strip().splitlines()[-1]
    return result == "0"


def start_workers(hosts, nodes):
    info("*** Starting NebulaStream workers inside benchmark nodes\n")
    for node in nodes:
        cmd = (
            f"bash -lc 'export PATH={CONTAINER_PATH}; "
            f"{WORKER_BIN} "
            f"--grpc=0.0.0.0:{WORKER_GRPC_PORT} "
            f"--data_address={node.management_ip}:{WORKER_DATA_PORT} "
            f"> /tmp/{node.name}.log 2>&1 &'"
        )
        info(f"***   {node.name}: {cmd}\n")
        hosts[node.name].cmd(cmd)


def wait_for_workers(hosts, nodes, timeout_s=20):
    info("*** Waiting for NebulaStream worker ports to become ready\n")
    deadline = time.time() + timeout_s
    pending = {node.name for node in nodes}
    while pending and time.time() < deadline:
        finished = set()
        for name in pending:
            output = hosts[name].cmd(
                f"grep -qi ':{WORKER_GRPC_PORT_HEX} ' /proc/net/tcp /proc/net/tcp6 2>/dev/null; echo $?"
            )
            if output.strip().splitlines()[-1] == "0":
                finished.add(name)
        pending -= finished
        if pending:
            time.sleep(1)
    if pending:
        show_worker_logs(hosts, nodes, lines=50)
        raise RuntimeError(f"Workers did not become ready on port {WORKER_GRPC_PORT}: {', '.join(sorted(pending))}")


def show_worker_logs(hosts, nodes, lines=20):
    info("*** Last worker log lines\n")
    for node in nodes:
        info(f"***   {node.name}\n")
        info(hosts[node.name].cmd(f"tail -n {lines} /tmp/{node.name}.log || true"))


def apply_link_constraints(runtimes):
    info("*** Applying link delay/bandwidth constraints\n")
    for runtime in runtimes:
        runtime.intf1.config(
            bw=runtime.spec.bandwidth_mbps,
            delay=f"{runtime.spec.latency_ms:g}ms",
        )
        runtime.intf2.config(
            bw=runtime.spec.bandwidth_mbps,
            delay=f"{runtime.spec.latency_ms:g}ms",
        )


def configure_management_ips(hosts, nodes, extra_hosts=None):
    info("*** Configuring management IPs on loopback\n")
    for node in list(nodes) + list(extra_hosts or []):
        host = hosts[node.name]
        if has_command(host, "ip"):
            host.cmd("ip link set lo up")
            host.cmd(f"ip addr replace {node.management_ip}/32 dev lo")
        else:
            host.cmd(f"ifconfig lo {node.management_ip} netmask 255.255.255.255 up")


def configure_link_interfaces(hosts, runtimes):
    info("*** Configuring point-to-point link addresses\n")
    for runtime in runtimes:
        left = hosts[runtime.spec.left]
        right = hosts[runtime.spec.right]
        if has_command(left, "ip"):
            left.cmd(f"ip addr flush dev {runtime.left_intf}")
            left.cmd(f"ip addr add {runtime.left_ip}/30 dev {runtime.left_intf}")
            left.cmd(f"ip link set dev {runtime.left_intf} up")
        else:
            left.cmd(f"ifconfig {runtime.left_intf} {runtime.left_ip} netmask 255.255.255.252 up")

        if has_command(right, "ip"):
            right.cmd(f"ip addr flush dev {runtime.right_intf}")
            right.cmd(f"ip addr add {runtime.right_ip}/30 dev {runtime.right_intf}")
            right.cmd(f"ip link set dev {runtime.right_intf} up")
        else:
            right.cmd(f"ifconfig {runtime.right_intf} {runtime.right_ip} netmask 255.255.255.252 up")


def compute_next_hop(source, target, adjacency):
    queue = deque([(source, None)])
    visited = {source}
    while queue:
        current, first_hop = queue.popleft()
        if current == target:
            return first_hop
        for neighbor in adjacency[current]:
            if neighbor in visited:
                continue
            visited.add(neighbor)
            queue.append((neighbor, neighbor if first_hop is None else first_hop))
    raise ValueError(f"No route between {source} and {target}")


def configure_routes(hosts, nodes, runtimes, extra_hosts=None):
    info("*** Configuring static routes\n")
    all_hosts = list(nodes) + list(extra_hosts or [])
    adjacency = {node.name: {} for node in all_hosts}
    management_ips = {node.name: node.management_ip for node in all_hosts}

    for runtime in runtimes:
        adjacency[runtime.spec.left][runtime.spec.right] = (runtime.left_intf, runtime.right_ip)
        adjacency[runtime.spec.right][runtime.spec.left] = (runtime.right_intf, runtime.left_ip)

    for source in all_hosts:
        for target in all_hosts:
            if source.name == target.name:
                continue
            next_hop = compute_next_hop(source.name, target.name, adjacency)
            out_intf, via_ip = adjacency[source.name][next_hop]
            host = hosts[source.name]
            if has_command(host, "ip"):
                host.cmd(f"ip route replace {management_ips[target.name]}/32 via {via_ip} dev {out_intf}")
            else:
                host.cmd(f"route add -host {management_ips[target.name]} gw {via_ip} dev {out_intf}")


def add_nodes(net, nodes, image, enforce_storage):
    info("*** Adding Docker containers\n")
    hosts = {}
    for node in nodes:
        effective_cpu = min(node.cpu, HOST_CPU_LIMIT)
        if effective_cpu != node.cpu:
            info(
                f"***   {node.name}: requested cpu={node.cpu} exceeds host capacity "
                f"{HOST_CPU_LIMIT}; using {effective_cpu}\n"
            )
        params = {
            "dimage": image,
            "dcmd": KEEPALIVE_CMD,
            "cpus": effective_cpu,
            "mem_limit": node.memory,
            "publish_all_ports": False,
            "privileged": True,
        }
        if enforce_storage:
            params["storage_opt"] = {"size": node.storage}
        hosts[node.name] = net.addDocker(node.name, **params)
    return hosts


def add_nebuli(net):
    info("*** Adding nebuli container\n")
    return net.addDocker(
        NEBULI_NAME,
        dimage=NEBULI_IMAGE,
        dcmd=KEEPALIVE_CMD,
        volumes=nebuli_volumes(),
        publish_all_ports=False,
        privileged=True,
    )


def add_links(net, hosts, links, subnets):
    info("*** Creating custom links\n")
    runtimes = []
    for link in links:
        subnet = next(subnets)
        link_object = net.addLink(
            hosts[link.left],
            hosts[link.right],
            cls=TCLink,
        )
        runtimes.append(
            LinkRuntime(
                spec=link,
                intf1=link_object.intf1,
                intf2=link_object.intf2,
                left_ip=str(subnet.network_address + 1),
                right_ip=str(subnet.network_address + 2),
                left_intf=link_object.intf1.name,
                right_intf=link_object.intf2.name,
            )
        )
    return runtimes


def add_nebuli_link(net, hosts, attach_to_name, subnets):
    info(f"*** Connecting {NEBULI_NAME} to {attach_to_name}\n")
    subnet = next(subnets)
    link_object = net.addLink(
        hosts[NEBULI_NAME],
        hosts[attach_to_name],
        cls=TCLink,
    )
    return LinkRuntime(
        spec=LinkSpec(
            left=NEBULI_NAME,
            right=attach_to_name,
            latency_ms=0,
            bandwidth_mbps=0,
        ),
        intf1=link_object.intf1,
        intf2=link_object.intf2,
        left_ip=str(subnet.network_address + 1),
        right_ip=str(subnet.network_address + 2),
        left_intf=link_object.intf1.name,
        right_intf=link_object.intf2.name,
    )


def add_nebuli_links(net, hosts, nodes, subnets):
    return [add_nebuli_link(net, hosts, node.name, subnets) for node in nodes]


def show_summary(nodes, links):
    info("*** Environment summary\n")
    for node in nodes:
        info(
            f"***   node {node.name}: cpu={node.cpu:g}, memory={node.memory}, "
            f"storage={node.storage}, management_ip={node.management_ip}\n"
        )
    for link in links:
        info(
            f"***   link {link.spec.left}<->{link.spec.right}: "
            f"delay={link.spec.latency_ms:g}ms, bw={link.spec.bandwidth_mbps:g}Mbps\n"
        )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a NebulaStream environment from config/config.yaml"
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--enforce-storage", action="store_true")
    parser.add_argument("--skip-cli", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    nodes, links = load_config(args.config.resolve())

    remove_existing_containers([node.name for node in nodes])

    net = Containernet(controller=Controller, ipBase="10.0.0.0/8")
    net.addController("c0")
    hosts = add_nodes(net, nodes, args.image, args.enforce_storage)
    hosts[NEBULI_NAME] = add_nebuli(net)
    subnets = LINK_SUBNET_POOL.subnets(new_prefix=30)
    runtimes = add_links(net, hosts, links, subnets)
    nebuli_runtimes = add_nebuli_links(net, hosts, nodes, subnets)
    all_runtimes = runtimes + nebuli_runtimes
    extra_hosts = [ExtraHostSpec(name=NEBULI_NAME, management_ip=NEBULI_MANAGEMENT_IP)]

    try:
        info("*** Starting network\n")
        net.start()
        install_debug_tools(hosts)
        apply_link_constraints(runtimes)
        configure_management_ips(hosts, nodes, extra_hosts=extra_hosts)
        configure_link_interfaces(hosts, all_runtimes)
        configure_hosts_file(hosts, nodes, extra_hosts=extra_hosts)
        configure_nebuli_direct_hosts(hosts, nebuli_runtimes)
        configure_routes(hosts, nodes, all_runtimes, extra_hosts=extra_hosts)
        start_workers(hosts, nodes)
        time.sleep(3)
        show_summary(nodes, runtimes)
        show_worker_logs(hosts, nodes)
        info("*** Useful checks\n")
        info(f"***   {nodes[0].name} ip -br addr\n")
        info(f"***   {nodes[0].name} ip route\n")
        info(f"***   {nodes[0].name} ping -c 2 {nodes[-1].name}\n")
        info(f"***   {nodes[0].name} tail -f /tmp/{nodes[0].name}.log\n")
        info(f"***   {NEBULI_NAME} ping -c 2 {nodes[0].name}\n")
        info(f"***   {NEBULI_NAME} getent hosts {nodes[0].name}\n")
        info(f"***   {NEBULI_NAME} tail -n 20 /etc/hosts\n")
        info(f"***   inside {NEBULI_NAME}: ls /queries\n")
        if not args.skip_cli:
            CLI(net)
    finally:
        info("*** Stopping network\n")
        net.stop()


if __name__ == "__main__":
    main()
