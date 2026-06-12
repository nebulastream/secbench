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
NEBULI_IMAGE = "nebulastream/nes-cli:distributed-poc"

# Adjust this if the worker binary inside the image has a different name.
WORKER_BIN = os.environ.get("NES_WORKER_BIN", "nes-single-node-worker")
QUERIES_DIR = os.environ.get("NES_QUERIES_DIR", os.path.join(os.getcwd(), "que"))

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

NEBULI = {
    "name": "nebuli",
    "ip": "10.1.1.10/24",
}


ALL_NODES = WORKERS + [NEBULI]


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


def install_debug_tools(nodes):
    info("*** Installing network debugging tools\n")
    for name, n in nodes.items():
        info(f"***   {name}\n")
        n.cmd(
            "apt-get update && "
            "DEBIAN_FRONTEND=noninteractive apt-get install -y "
            "iputils-ping iproute2 net-tools"
        )


def configure_mininet_ips(nodes):
    info("*** Configuring Mininet-facing interfaces\n")
    for entry in ALL_NODES:
        name = entry["name"]
        ip = entry["ip"]
        intf = f"{name}-eth0"
        info(f"***   {name}: configuring {intf} -> {ip}\n")
        nodes[name].cmd(f"ip link set dev {intf} up")
        nodes[name].cmd(f"ip addr flush dev {intf}")
        nodes[name].cmd(f"ip addr add {ip} dev {intf}")


def configure_hosts_file(nodes):
    info("*** Configuring /etc/hosts entries\n")
    host_map = {entry["name"]: ip_without_prefix(entry["ip"]) for entry in ALL_NODES}
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
        info(n.cmd("tail -n 8 /etc/hosts"))


def start_workers(nodes):
    info("*** Starting NebulaStream workers inside the containers\n")
    for worker in WORKERS:
        name = worker["name"]
        connection_ip = ip_without_prefix(worker["ip"])
        cmd = (
            f"{WORKER_BIN} "
            f"--grpc=0.0.0.0:{worker['grpc_port']} "
            f"--data={connection_ip}:{worker['connection_port']} "
            f"> /tmp/{name}.log 2>&1 &"
        )
        info(f"*** {name}: {cmd}\n")
        nodes[name].cmd(cmd)


def nebuli_volumes():
    if os.path.isdir(QUERIES_DIR):
        return [f"{os.path.abspath(QUERIES_DIR)}:/queries:ro"]
    info(f"*** Warning: queries directory not found at {QUERIES_DIR}; starting nebuli without /queries mount\n")
    return []


if __name__ == "__main__":
    remove_existing_containers([entry["name"] for entry in ALL_NODES])

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

    info("*** Adding nebuli container\n")
    nodes[NEBULI["name"]] = net.addDocker(
        NEBULI["name"],
        dimage=NEBULI_IMAGE,
        dcmd="bash -lc 'trap : TERM INT; while true; do sleep 3600; done'",
        volumes=nebuli_volumes(),
        privileged=True,
    )

    info("*** Adding switch\n")
    sw1 = net.addSwitch("sw1", cls=OVSSwitch, failMode="standalone")

    info("*** Creating links\n")
    for entry in ALL_NODES:
        net.addLink(nodes[entry["name"]], sw1, cls=TCLink)

    info("*** Starting network\n")
    net.start()

    time.sleep(2)

    install_debug_tools(nodes)
    configure_mininet_ips(nodes)
    configure_hosts_file(nodes)
    show_network_state(nodes)

    #info("*** Basic connectivity check\n")
    #info(net.ping([nodes[entry["name"]] for entry in ALL_NODES]))

    start_workers(nodes)

    time.sleep(3)

    info("*** Last worker log lines\n")
    for worker in WORKERS:
        name = worker["name"]
        info(f"*** {name}\n")
        info(nodes[name].cmd(f"tail -n 20 /tmp/{name}.log || true"))

    info("*** Useful checks:\n")
    info("***   nebuli ping -c 3 node1\n")
    info("***   nebuli getent hosts node1\n")
    info("***   sh docker exec -it mn.nebuli bash\n")
    info("***   inside nebuli: ls /queries\n")
    info("***   inside nebuli: nes-cli -d -t /queries/topology.yaml register -x -i /queries/query.yaml\n")
    info("***   node1 tail -f /tmp/node1.log\n")
    info("***   node2 tail -f /tmp/node2.log\n")
    info("***   node3 tail -f /tmp/node3.log\n")
    info("*** Running CLI\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()
