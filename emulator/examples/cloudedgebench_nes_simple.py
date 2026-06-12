#!/usr/bin/env python3

import argparse
import os
import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import yaml
from mininet.net import Containernet
from mininet.node import Controller, OVSSwitch
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
LINK_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+:[A-Za-z0-9_-]+$")
HOST_CPU_LIMIT = os.cpu_count() or 1
IP_PREFIX = "10.1.1"
PREFIX_LEN = 24
NEBULI_NAME = "nebuli"
NEBULI_IP = f"{IP_PREFIX}.254"
WORKER_GRPC_PORT = 8080
WORKER_DATA_PORT = 9090
CONTAINER_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"


@dataclass
class NodeSpec:
    name: str
    cpu: int
    memory: str
    storage: str
    ip: str


@dataclass
class LinkSpec:
    left: str
    right: str
    latency_ms: float
    bandwidth_mbps: float


@dataclass
class AccessLinkSpec:
    delay: str | None
    bandwidth: float | None


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
    nodes = [
        NodeSpec(
            name=entry["name"],
            cpu=int(entry["cpu"]),
            memory=str(entry["memory"]),
            storage=str(entry["storage"]),
            ip=f"{IP_PREFIX}.{index}",
        )
        for index, entry in enumerate(config["heterogeneity"], start=1)
    ]

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

    return nodes, links


def access_link_specs(nodes, links):
    specs = {node.name: AccessLinkSpec(delay=None, bandwidth=None) for node in nodes}
    for link in links:
        for node_name in (link.left, link.right):
            if specs[node_name].delay is None:
                specs[node_name] = AccessLinkSpec(
                    delay=f"{link.latency_ms:g}ms",
                    bandwidth=link.bandwidth_mbps,
                )
    return specs


def add_worker_nodes(net, nodes, image, enforce_storage):
    info("*** Adding worker containers\n")
    hosts = {}
    for index, node in enumerate(nodes, start=1):
        effective_cpu = min(node.cpu, HOST_CPU_LIMIT)
        if effective_cpu != node.cpu:
            info(
                f"***   {node.name}: requested cpu={node.cpu} exceeds host capacity "
                f"{HOST_CPU_LIMIT}; using {effective_cpu}\n"
            )
        grpc_host_port = WORKER_GRPC_PORT + index - 1
        data_host_port = WORKER_DATA_PORT + index - 1
        params = {
            "dimage": image,
            "dcmd": KEEPALIVE_CMD,
            "cpus": effective_cpu,
            "mem_limit": node.memory,
            "ports": [WORKER_GRPC_PORT, WORKER_DATA_PORT],
            "port_bindings": {
                grpc_host_port: WORKER_GRPC_PORT,
                data_host_port: WORKER_DATA_PORT,
            },
            "publish_all_ports": True,
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


def add_switch_links(net, hosts, nodes, links):
    specs = access_link_specs(nodes, links)
    sw = net.addSwitch("sw1", cls=OVSSwitch, failMode="standalone")

    info("*** Creating switch links\n")
    for node in nodes:
        spec = specs[node.name]
        kwargs = {"cls": TCLink}
        if spec.delay is not None:
            kwargs["delay"] = spec.delay
        if spec.bandwidth is not None:
            kwargs["bw"] = spec.bandwidth
        net.addLink(hosts[node.name], sw, **kwargs)

    net.addLink(hosts[NEBULI_NAME], sw, cls=TCLink)


def install_debug_tools(hosts, install_tools=False):
    action = "Installing" if install_tools else "Checking"
    info(f"*** {action} network tools inside nodes\n")
    for name, host in hosts.items():
        info(f"***   {name}\n")
        if install_tools:
            host.cmd(
                "bash -lc 'export PATH="
                + CONTAINER_PATH
                + "; apt-get update && "
                + "DEBIAN_FRONTEND=noninteractive apt-get install -y "
                + "iputils-ping iproute2 net-tools'"
            )
        has_ip = host.cmd(f"bash -lc 'export PATH={CONTAINER_PATH}; command -v ip >/dev/null 2>&1; echo $?'").strip().splitlines()[-1]
        has_ping = host.cmd(f"bash -lc 'export PATH={CONTAINER_PATH}; command -v ping >/dev/null 2>&1; echo $?'").strip().splitlines()[-1]
        if has_ip != "0" or has_ping != "0":
            info(f"*** Warning: {name} is missing ip and/or ping; use --install-tools or add iproute2/iputils-ping to the image\n")


def configure_ips(hosts, nodes):
    info("*** Configuring Mininet-facing IP addresses\n")
    for node in nodes:
        intf = f"{node.name}-eth0"
        hosts[node.name].cmd(f"ip link set dev {intf} up")
        hosts[node.name].cmd(f"ip addr flush dev {intf}")
        hosts[node.name].cmd(f"ip addr add {node.ip}/{PREFIX_LEN} dev {intf}")

    nebuli_intf = f"{NEBULI_NAME}-eth0"
    hosts[NEBULI_NAME].cmd(f"ip link set dev {nebuli_intf} up")
    hosts[NEBULI_NAME].cmd(f"ip addr flush dev {nebuli_intf}")
    hosts[NEBULI_NAME].cmd(f"ip addr add {NEBULI_IP}/{PREFIX_LEN} dev {nebuli_intf}")


def configure_hosts_file(hosts, nodes):
    info("*** Configuring /etc/hosts entries\n")
    entries = [(node.ip, node.name) for node in nodes] + [(NEBULI_IP, NEBULI_NAME)]
    for host in hosts.values():
        for ip, name in entries:
            host.cmd(
                "sh -lc \"grep -q '^%s %s$' /etc/hosts || echo '%s %s' >> /etc/hosts\""
                % (ip, name, ip, name)
            )


def start_workers(hosts, nodes):
    info("*** Starting NebulaStream workers inside benchmark nodes\n")
    for node in nodes:
        cmd = (
            f"bash -lc 'export PATH={CONTAINER_PATH}; "
            f"{WORKER_BIN} "
            f"--grpc=0.0.0.0:{WORKER_GRPC_PORT} "
            f"--data_address={node.ip}:{WORKER_DATA_PORT} "
            f"> /tmp/{node.name}.log 2>&1 &'"
        )
        info(f"***   {node.name}: {cmd}\n")
        hosts[node.name].cmd(cmd)


def show_worker_logs(hosts, nodes, lines=20):
    info("*** Last worker log lines\n")
    for node in nodes:
        info(f"***   {node.name}\n")
        info(hosts[node.name].cmd(f"tail -n {lines} /tmp/{node.name}.log || true"))


def show_summary(nodes, links):
    info("*** Environment summary\n")
    for node in nodes:
        info(
            f"***   node {node.name}: cpu={node.cpu:g}, memory={node.memory}, "
            f"storage={node.storage}, ip={node.ip}\n"
        )
    for link in links:
        info(
            f"***   configured link {link.left}<->{link.right}: "
            f"latency={link.latency_ms:g}ms, bw={link.bandwidth_mbps:g}Mbps\n"
        )
    info("*** Note: using one shared switch like dist-nes.py; configured link values are applied as per-node access-link shaping.\n")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create a simple NebulaStream environment from config/config.yaml"
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--enforce-storage", action="store_true")
    parser.add_argument("--install-tools", action="store_true")
    parser.add_argument("--skip-cli", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    nodes, links = load_config(args.config.resolve())
    all_container_names = [node.name for node in nodes] + [NEBULI_NAME]

    remove_existing_containers(all_container_names)

    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")
    info("*** Adding controller\n")
    net.addController("c0")

    hosts = add_worker_nodes(net, nodes, args.image, args.enforce_storage)
    hosts[NEBULI_NAME] = add_nebuli(net)
    add_switch_links(net, hosts, nodes, links)

    try:
        info("*** Starting network\n")
        net.start()
        time.sleep(2)

        install_debug_tools(hosts, install_tools=args.install_tools)
        configure_ips(hosts, nodes)
        configure_hosts_file(hosts, nodes)
        start_workers(hosts, nodes)

        time.sleep(3)
        show_summary(nodes, links)
        show_worker_logs(hosts, nodes)

        info("*** Useful checks\n")
        info(f"***   {nodes[0].name} ping -c 2 {nodes[-1].name}\n")
        info(f"***   {NEBULI_NAME} ping -c 2 {nodes[0].name}\n")
        info(f"***   {NEBULI_NAME} getent hosts {nodes[0].name}\n")
        info(f"***   {nodes[0].name} tail -f /tmp/{nodes[0].name}.log\n")
        info(f"***   inside {NEBULI_NAME}: ls /queries\n")

        if not args.skip_cli:
            CLI(net)
    finally:
        info("*** Stopping network\n")
        net.stop()


if __name__ == "__main__":
    main()
