#!/usr/bin/env python3

from mininet.net import Containernet
from mininet.node import Controller, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker
import time
import os

setLogLevel("info")

IMAGE = "nebulastream/worker:tt-tcp-sink"
NEBULI_IMAGE = "nebulastream/nes-cli:tt-tcp-sink"

# Adjust this if the worker binary inside the image has a different name.
WORKER_BIN = os.environ.get("NES_WORKER_BIN", "nes-single-node-worker")
QUERIES_DIR = os.environ.get("NES_QUERIES_DIR", os.path.join(os.getcwd(), "que"))
MODEL_DIR = os.environ.get("NES_MODEL_DIR", os.path.join(os.getcwd(), "model"))
CONTAINER_MODEL_SOURCE_DIR = "/tmp/host-model"

WORKER = {
    "name": "node1",
    "ip": "10.1.1.1/24",
    "grpc_port": 8080,
    "connection_port": 9090,
    "host_port_bindings": {8080: 8080, 9090: 9090},
}

NEBULI = {
    "name": "nebuli",
    "ip": "10.1.1.10/24",
}

ALL_NODES = [WORKER, NEBULI]


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


def sync_models(nodes):
    if not os.path.isdir(MODEL_DIR):
        return
    info("*** Copying model files into containers\n")
    for entry in ALL_NODES:
        name = entry["name"]
        info(f"***   {name}\n")
        nodes[name].cmd(
            f"mkdir -p /model && cp -a {CONTAINER_MODEL_SOURCE_DIR}/. /model/"
        )


def start_worker(nodes):
    info("*** Starting NebulaStream worker inside the container\n")
    name = WORKER["name"]
    connection_ip = ip_without_prefix(WORKER["ip"])
    cmd = (
        f"{WORKER_BIN} "
        f"--grpc=0.0.0.0:{WORKER['grpc_port']} "
        f"--data_address={connection_ip}:{WORKER['connection_port']} "
        f"> /tmp/{name}.log 2>&1 &"
    )
    info(f"*** {name}: {cmd}\n")
    nodes[name].cmd(cmd)


def nebuli_volumes():
    volumes = []
    if os.path.isdir(QUERIES_DIR):
        volumes.append(f"{os.path.abspath(QUERIES_DIR)}:/queries:ro")
    else:
        info(
            f"*** Warning: queries directory not found at {QUERIES_DIR}; starting nebuli without /queries mount\n"
        )
    if os.path.isdir(MODEL_DIR):
        volumes.append(
            f"{os.path.abspath(MODEL_DIR)}:{CONTAINER_MODEL_SOURCE_DIR}:ro"
        )
    else:
        info(
            f"*** Warning: model directory not found at {MODEL_DIR}; starting nebuli without model copy source\n"
        )
    return volumes


def worker_volumes():
    if os.path.isdir(MODEL_DIR):
        return [f"{os.path.abspath(MODEL_DIR)}:{CONTAINER_MODEL_SOURCE_DIR}:ro"]
    info(
        f"*** Warning: model directory not found at {MODEL_DIR}; starting worker without model copy source\n"
    )
    return []


if __name__ == "__main__":
    remove_existing_containers([entry["name"] for entry in ALL_NODES])

    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")
    info("*** Adding controller\n")
    net.addController("c0")

    info("*** Adding worker container\n")
    nodes = {}
    nodes[WORKER["name"]] = net.addDocker(
        WORKER["name"],
        dimage=IMAGE,
        ports=[WORKER["grpc_port"], WORKER["connection_port"]],
        port_bindings=WORKER["host_port_bindings"],
        volumes=worker_volumes(),
        publish_all_ports=True,
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
    sync_models(nodes)
    show_network_state(nodes)

    start_worker(nodes)

    time.sleep(3)

    info("*** Last worker log lines\n")
    info(f"*** {WORKER['name']}\n")
    info(nodes[WORKER["name"]].cmd(f"tail -n 20 /tmp/{WORKER['name']}.log || true"))

    info("*** Useful checks:\n")
    info("***   nebuli ping -c 3 node1\n")
    info("***   nebuli getent hosts node1\n")
    info("***   sh docker exec -it mn.nebuli bash\n")
    info("***   inside nebuli: ls /queries\n")
    info("***   inside nebuli: ls /model\n")
    info("***   inside nebuli: nes-cli -d -t /queries/topology.yaml register -x -i /queries/query.yaml\n")
    info("***   node1 tail -f /tmp/node1.log\n")
    info("*** Running CLI\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()
