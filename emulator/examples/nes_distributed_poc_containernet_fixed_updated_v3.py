#!/usr/bin/env python3

from mininet.net import Containernet
from mininet.node import Controller, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker
import time
import os

setLogLevel('info')

IMAGE = "nebulastream/worker:distributed-poc"

# Adjust this if the worker binary inside the image has a different name.
# The NebulaStream docs often use `nes-single-node-worker` for distributed mode.
WORKER_BIN = os.environ.get("NES_WORKER_BIN", "nes-single-node-worker")

WORKERS = [
    {
        "name": "node1",
        "ip": "10.1.1.1/24",
        "grpc_port": 8080,
        "connection_port": 9090,
        "host_port_bindings": {8080: 8080, 9090: 9090},
    },
    {
        "name": "node2",
        "ip": "10.1.1.2/24",
        "grpc_port": 8080,
        "connection_port": 9090,
        "host_port_bindings": {8081: 8080, 9091: 9090},
    },
    {
        "name": "node3",
        "ip": "10.1.1.3/24",
        "grpc_port": 8080,
        "connection_port": 9090,
        "host_port_bindings": {8082: 8080, 9092: 9090},
    },
]


def remove_existing_containers(container_names):
    client = docker.from_env()
    for name in container_names:
        try:
            c = client.containers.get(f"mn.{name}")
            info(f"*** Removing existing container mn.{name}\n")
            c.stop()
            c.remove(force=True)
        except docker.errors.NotFound:
            pass
        except Exception as e:
            info(f"*** Warning: could not remove mn.{name}: {e}\n")


def ip_without_prefix(cidr_ip):
    return cidr_ip.split("/")[0]


def prefix_length(cidr_ip):
    return int(cidr_ip.split("/")[1])


def configure_mininet_ips(nodes):
    info("*** Configuring Mininet-facing IP addresses\n")
    for worker in WORKERS:
        name = worker["name"]
        ip = ip_without_prefix(worker["ip"])
        plen = prefix_length(worker["ip"])
        intf = nodes[name].defaultIntf()
        info(f"***   {name}: setting {ip}/{plen} on {intf}\n")
        nodes[name].setIP(ip=ip, prefixLen=plen, intf=intf)
        nodes[name].cmd(f"ifconfig {intf} up || true")


def install_debug_tools(nodes):
    info("*** Installing network debugging tools\n")
    for name, n in nodes.items():
        info(f"***   {name}\n")
        n.cmd(
            "apt-get update && "
            "DEBIAN_FRONTEND=noninteractive apt-get install -y "
            "iputils-ping iproute2 net-tools"
        )


def configure_hosts_file(nodes):
    info("*** Configuring /etc/hosts entries\n")
    host_map = {w["name"]: ip_without_prefix(w["ip"]) for w in WORKERS}
    for n in nodes.values():
        for host, ip in host_map.items():
            n.cmd(
                "sh -lc \"grep -q '^%s %s$' /etc/hosts || echo '%s %s' >> /etc/hosts\""
                % (ip, host, ip, host)
            )


def show_network_state(nodes):
    info("*** Container interface summary\n")
    for name, n in nodes.items():
        info(f"*** {name} ifconfig -a\n")
        info(n.cmd("ifconfig -a || true"))
        info(f"*** {name} ip -br addr\n")
        info(n.cmd("ip -br addr || true"))
        info(f"*** {name} ip route\n")
        info(n.cmd("ip route || true"))
        info(f"*** {name} /etc/hosts\n")
        info(n.cmd("tail -n 5 /etc/hosts"))


def start_workers(nodes):
    info("*** Starting NebulaStream workers inside the containers\n")
    for worker in WORKERS:
        name = worker["name"]
        connection_ip = ip_without_prefix(worker["ip"])
        cmd = (
            f"{WORKER_BIN} "
            f"--grpc=0.0.0.0:{worker['grpc_port']} "
            f"--connection={connection_ip}:{worker['connection_port']} "
            f"> /tmp/{name}.log 2>&1 &"
        )
        info(f"*** {name}: {cmd}\n")
        nodes[name].cmd(cmd)


if __name__ == "__main__":
    remove_existing_containers([w["name"] for w in WORKERS])

    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")
    info("*** Adding controller\n")
    net.addController("c0")

    info("*** Adding worker containers\n")
    nodes = {}
    for worker in WORKERS:
        nodes[worker["name"]] = net.addDocker(
            worker["name"],
            dimage=IMAGE,
            ports=[worker["grpc_port"], worker["connection_port"]],
            port_bindings=worker["host_port_bindings"],
            publish_all_ports=False,
            privileged=True,
        )

    info("*** Adding switch\n")
    sw1 = net.addSwitch("sw1", cls=OVSSwitch, failMode="standalone")

    info("*** Creating links\n")
    for worker in WORKERS:
        net.addLink(nodes[worker["name"]], sw1, cls=TCLink)

    info("*** Starting network\n")
    net.start()

    # Give OVS and veth setup a moment before assigning addresses and testing.
    time.sleep(2)

    install_debug_tools(nodes)
    configure_mininet_ips(nodes)
    configure_hosts_file(nodes)
    show_network_state(nodes)

    info("*** Basic connectivity check\n")
    info(net.ping([nodes["node1"], nodes["node2"], nodes["node3"]]))

    start_workers(nodes)

    time.sleep(3)

    info("*** Last worker log lines\n")
    for worker in WORKERS:
        name = worker["name"]
        info(f"*** {name}\n")
        info(nodes[name].cmd(f"tail -n 20 /tmp/{name}.log || true"))

    info("*** Useful checks:\n")
    info("***   node1 ifconfig -a\n")
    info("***   node1 ip -br addr\n")
    info("***   node1 ping -c 3 node2\n")
    info("***   node1 ping -c 3 10.1.1.2\n")
    info("***   node1 ps aux\n")
    info("***   node1 tail -f /tmp/node1.log\n")
    info("***   node2 tail -f /tmp/node2.log\n")
    info("***   node3 tail -f /tmp/node3.log\n")
    info("*** Running CLI\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()
