"""Validate NebulaStream environment YAML and expand it into a deployment plan."""

from dataclasses import asdict, dataclass, field
from collections import deque
from ipaddress import IPv4Network
import math
from pathlib import Path
import re

import yaml


class ConfigError(ValueError):
    pass


def mapping(value, label, allowed):
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be a mapping")
    unknown = set(value) - set(allowed.split())
    if unknown:
        raise ConfigError(f"Unknown {label} keys: {', '.join(map(str, unknown))}")
    return value


def number(value, label, minimum=0, maximum=None, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{label} must be a number")
    if not math.isfinite(value) or value < minimum or (maximum is not None and value > maximum):
        raise ConfigError(f"{label} is out of range")
    if integer and not isinstance(value, int):
        raise ConfigError(f"{label} must be an integer")
    return value


def name(value):
    # Leave room for Linux's 15-character interface names, even at high degree.
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,6}", value):
        raise ConfigError(f"Invalid node id {value!r}; use 1-7 alphanumeric, '_' or '-' characters")
    return value


def size(value, label):
    if not isinstance(value, str) or not re.fullmatch(r"[1-9][0-9]*[bkmgBKMG]", value):
        raise ConfigError(f"{label} must be a positive Docker size, e.g. 512m or 2g")
    return value.lower()


@dataclass(frozen=True)
class Host:
    id: str
    ip: str
    cpu: float
    memory: str
    storage: str | None = None


@dataclass(frozen=True)
class Link:
    left: str
    right: str
    latency: float = 0
    bandwidth: float | None = None
    loss: float = 0
    left_ip: str = ""
    right_ip: str = ""
    left_interface: str = ""
    right_interface: str = ""

    def tc_params(self):
        params = {"delay": f"{self.latency:g}ms", "loss": self.loss}
        if self.bandwidth is not None:
            params["bw"] = self.bandwidth
        return params


@dataclass(frozen=True)
class Plan:
    workers: list[Host]
    cli: Host
    links: list[Link]
    subnet: str
    routes: list[dict]
    deployment: dict
    services: list[Host] = field(default_factory=list)
    service_settings: dict = field(default_factory=dict)
    mounts: dict = field(default_factory=dict)

    @property
    def all_hosts(self):
        return [*self.workers, *self.services, self.cli]

    def as_dict(self):
        return asdict(self)


def link(left, right, settings):
    mapping(settings, "link", "latency bandwidth loss")
    latency = number(settings.get("latency", 0), "link.latency")
    bandwidth = settings.get("bandwidth")
    if bandwidth is not None:
        # The launcher's TCIntf subclass supports links above the stock 1 Gbps cap.
        bandwidth = number(bandwidth, "link.bandwidth", 0.001, 1000_000)
    loss = number(settings.get("loss", 0), "link.loss", 0, 100)
    return Link(left, right, latency, bandwidth, loss)


def load_plan(path):
    path = Path(path).resolve()
    try:
        config = yaml.safe_load(path.read_text())
    except (OSError, yaml.YAMLError) as exc:
        raise ConfigError(f"Cannot read {path}: {exc}") from exc
    return make_plan(config, path.parent)


def make_plan(config, base_dir, service_settings=None):
    mapping(config, "configuration", "hardware network deployment")
    deployment = dict(mapping(config.get("deployment", {}), "deployment",
        "worker_image cli_image worker_binary worker_args queries_dir model_dir "
        "startup_timeout publish_ports grpc_port data_port host_grpc_port_base "
        "cli_cpu cli_memory install_tools"))
    defaults = {
        "worker_image": "nebulastream/worker:tt-tcp-sink",
        "cli_image": "nebulastream/nes-cli:tt-tcp-sink",
        "worker_binary": "nes-single-node-worker", "worker_args": [],
        "startup_timeout": 90, "publish_ports": False,
        "grpc_port": 8080, "data_port": 9090,
        "host_grpc_port_base": 18080, "install_tools": False,
        "cli_cpu": 0.5, "cli_memory": "512m",
    }
    deployment = defaults | deployment
    for key in ("worker_image", "cli_image", "worker_binary"):
        if not isinstance(deployment[key], str) or not deployment[key].strip():
            raise ConfigError(f"deployment.{key} must be a nonempty string")
    args = deployment["worker_args"]
    if not isinstance(args, list) or any(not isinstance(arg, str) for arg in args):
        raise ConfigError("deployment.worker_args must be a list of strings")
    if any(arg.split("=", 1)[0] in ("--grpc", "--data_address", "--data") for arg in args):
        raise ConfigError("Configure worker endpoints through grpc_port and data_port")
    number(deployment["startup_timeout"], "startup_timeout", 1)
    number(deployment["cli_cpu"], "cli_cpu", 0.001)
    size(deployment["cli_memory"], "cli_memory")
    if not isinstance(deployment["publish_ports"], bool):
        raise ConfigError("publish_ports must be true or false")
    if not isinstance(deployment["install_tools"], bool):
        raise ConfigError("install_tools must be true or false")
    for key in ("grpc_port", "data_port", "host_grpc_port_base"):
        number(deployment[key], key, 1, 65535, integer=True)
    if deployment["grpc_port"] == deployment["data_port"]:
        raise ConfigError("grpc_port and data_port must differ")
    for key in ("queries_dir", "model_dir"):
        value = deployment.get(key)
        if value is not None:
            if not isinstance(value, str) or not value or ":" in value:
                raise ConfigError(f"{key} must be a directory path without ':'")
            resolved = (Path(base_dir) / value).resolve()
            if not resolved.is_dir():
                raise ConfigError(f"{key} does not exist: {resolved}")
            deployment[key] = str(resolved)

    network = mapping(config.get("network"), "network",
        "topology subnet link_subnet links default_link cli_link cli_node branching_factor")
    try:
        subnet = IPv4Network(network.get("subnet", "10.1.1.0/24"))
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"Invalid network.subnet: {exc}") from exc
    if subnet.prefixlen > 29:
        raise ConfigError("network.subnet must have prefix length <= 29")
    hardware = config.get("hardware")
    if not isinstance(hardware, list) or not hardware:
        raise ConfigError("hardware must be a nonempty list")
    workers = []
    for entry in hardware:
        mapping(entry, "hardware entry", "id count cpu memory storage")
        identifier = name(entry.get("id"))
        count = number(entry.get("count", 1), "hardware.count", 1, 4096, integer=True)
        cpu = number(entry.get("cpu"), "hardware.cpu", 0.001)
        memory = size(entry.get("memory"), "hardware.memory")
        storage = size(entry["storage"], "hardware.storage") if "storage" in entry else None
        for index in range(count):
            worker_name = name(f"{identifier}{index + 1}" if "count" in entry else identifier)
            if len(workers) >= min(4096, subnet.num_addresses - 3):
                raise ConfigError("Too many workers for the subnet (or the 4096-worker limit)")
            ip = str(subnet.network_address + len(workers) + 1)
            workers.append(Host(worker_name, ip, cpu, memory, storage))
    cli = Host("nebuli", str(subnet.broadcast_address - 1), deployment["cli_cpu"], deployment["cli_memory"])
    service_settings = service_settings or {}
    services = []
    for identifier, settings in service_settings.items():
        if len(workers) + len(services) >= subnet.num_addresses - 3:
            raise ConfigError("Not enough service addresses for experiment containers")
        services.append(Host(name(identifier), str(subnet.network_address + len(workers) + len(services) + 1),
                             number(settings.get("cpu", 1), "service.cpu", 0.001),
                             size(settings.get("memory", "1g"), "service.memory")))
    worker_names = [worker.id for worker in workers]
    host_names = worker_names + [host.id for host in services] + [cli.id]
    if len(set(host_names)) != len(host_names):
        raise ConfigError("Host ids must be unique; 'nebuli' and configured service ids are reserved")
    if deployment["publish_ports"]:
        if deployment["host_grpc_port_base"] + len(workers) - 1 > 65535:
            raise ConfigError("Published worker port range exceeds 65535")

    topology = network.get("topology", "custom")
    defaults = network.get("default_link", {})
    cli_settings = network.get("cli_link", {})
    for settings in (defaults, cli_settings):
        link("", "", settings)
    links = []
    if topology == "custom":
        entries = network.get("links", [])
        if not isinstance(entries, list):
            raise ConfigError("network.links must be a list")
        for entry in entries:
            mapping(entry, "network.links entry", "link latency bandwidth loss")
            endpoints = entry.get("link", "")
            if not isinstance(endpoints, str) or len(endpoints.split(":")) != 2:
                raise ConfigError("Each link requires link: node1:node2")
            endpoints = endpoints.split(":")
            left, right = [name(endpoint) for endpoint in endpoints]
            if cli.id in (left, right):
                raise ConfigError("Configure the nebuli attachment through cli_node and cli_link")
            links.append(link(left, right, defaults | {k: v for k, v in entry.items() if k != "link"}))
    elif topology in ("star", "line", "ring", "tree"):
        branching = number(network.get("branching_factor", 2), "branching_factor", 1, 4096, integer=True)
        for i in range(1, len(workers)):
            parent = 0 if topology == "star" else (i - 1) // branching if topology == "tree" else i - 1
            links.append(link(workers[parent].id, workers[i].id, defaults))
        if topology == "ring":
            if len(workers) < 3:
                raise ConfigError("ring topology requires at least three workers")
            links.append(link(workers[-1].id, workers[0].id, defaults))
    else:
        raise ConfigError("network.topology must be custom, star, line, ring, or tree")
    if topology != "custom" and "links" in network:
        raise ConfigError("links may only be specified with topology: custom")
    if topology != "tree" and "branching_factor" in network:
        raise ConfigError("branching_factor may only be specified with topology: tree")
    cli_node = network.get("cli_node", workers[0].id)
    if cli_node not in worker_names:
        raise ConfigError("cli_node must name a worker")
    for host in services:
        settings = service_settings[host.id]
        if settings["attach_to"] not in worker_names:
            raise ConfigError(f"{host.id}.attach_to must name a worker")
        links.append(link(host.id, settings["attach_to"], settings.get("link", {})))
    links.append(link(cli.id, cli_node, cli_settings))
    adjacent = {identifier: [] for identifier in host_names}
    for entry in links:
        if entry.left not in adjacent or entry.right not in adjacent:
            raise ConfigError(f"Unknown link endpoint: {entry.left}, {entry.right}")
        if entry.left == entry.right or entry.right in adjacent[entry.left]:
            raise ConfigError("Self-links and duplicate links are not supported")
        adjacent[entry.left].append(entry.right)
        adjacent[entry.right].append(entry.left)
    seen, pending = set(), [cli.id]
    while pending:
        current = pending.pop()
        if current not in seen:
            seen.add(current)
            pending.extend(set(adjacent[current]) - seen)
    if len(seen) != len(host_names):
        raise ConfigError("Network must be connected")
    try:
        link_pool = IPv4Network(network.get("link_subnet", "10.200.0.0/16"))
    except (ValueError, TypeError) as exc:
        raise ConfigError(f"Invalid network.link_subnet: {exc}") from exc
    if link_pool.overlaps(subnet) or link_pool.num_addresses < 4 * len(links):
        raise ConfigError("link_subnet must not overlap subnet and must hold one /30 per link")
    numbered_links, next_hops = [], {}
    interface_counts = dict.fromkeys(host_names, 0)
    for index, entry in enumerate(links):
        ips = [str(link_pool.network_address + 4 * index + offset) for offset in (1, 2)]
        interfaces = []
        for identifier in (entry.left, entry.right):
            interfaces.append(f"{identifier}-eth{interface_counts[identifier]}")
            interface_counts[identifier] += 1
        numbered_links.append(Link(entry.left, entry.right, entry.latency, entry.bandwidth,
                                   entry.loss, *ips, *interfaces))
        next_hops[entry.left, entry.right] = (ips[1], interfaces[0])
        next_hops[entry.right, entry.left] = (ips[0], interfaces[1])
    routes = []
    addresses = {host.id: host.ip for host in [*workers, *services, cli]}
    for source in host_names:
        # Minimum-hop routing with deterministic ties in configuration link order.
        visited = {source}
        queue = deque((neighbor, neighbor) for neighbor in adjacent[source])
        while queue:
            destination, first_hop = queue.popleft()
            if destination in visited:
                continue
            visited.add(destination)
            gateway, interface = next_hops[source, first_hop]
            routes.append({"host": source, "destination": addresses[destination],
                           "gateway": gateway, "interface": interface, "source": addresses[source]})
            queue.extend((neighbor, first_hop) for neighbor in adjacent[destination] if neighbor not in visited)
    return Plan(workers, cli, numbered_links, str(subnet), routes, deployment, services, service_settings)
