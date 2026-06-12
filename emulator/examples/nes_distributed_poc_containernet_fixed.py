#!/usr/bin/env python3

from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker
import time
import os

setLogLevel('info')

IMAGE = "nebulastream/worker:distributed-poc"

# Adjust this if the worker binary inside the image has a different name.
# The NebulaStream docs use `nes-single-node-worker` for distributed mode.
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


if __name__ == "__main__":
    remove_existing_containers([w["name"] for w in WORKERS])

    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")
    info("*** Adding controller\n")
    net.addController("c0")

    info("*** Adding worker containers\n")
    nodes = {}
    for worker in WORKERS:
        # Do NOT pass NebulaStream flags as the Docker start command here.
        # Containernet overrides image CMD/ENTRYPOINT behavior, so we keep the
        # container alive with its default shell and start the worker later via cmd().
        nodes[worker["name"]] = net.addDocker(
            worker["name"],
            ip=worker["ip"],
            dimage=IMAGE,
            ports=[worker["grpc_port"], worker["connection_port"]],
            port_bindings=worker["host_port_bindings"],
            publish_all_ports=False,
            privileged=True,
        )

    info("*** Adding switch\n")
    sw1 = net.addSwitch("sw1")

    info("*** Creating links\n")
    for worker in WORKERS:
        net.addLink(nodes[worker["name"]], sw1, cls=TCLink)

    info("*** Starting network\n")
    net.start()
    
    HOSTS = {
        "node1": "10.1.1.1",
        "node2": "10.1.1.2",
        "node3": "10.1.1.3",
    }

    for n in nodes.values():
        for host, ip in HOSTS.items():
            n.cmd(f"sh -lc \"grep -q '^%s %s$' /etc/hosts || echo '%s %s' >> /etc/hosts\"" % (ip, host, ip, host))
    
    for name, n in nodes.items():
        info(f"*** Installing ping in {name}\n")
        n.cmd("apt-get update && apt-get install -y iputils-ping")

    info("*** Starting NebulaStream workers inside the containers\n")
    for worker in WORKERS:
        name = worker["name"]

        # Try the command style you used in docker compose / docker run first.
        cmd = (
            f"{WORKER_BIN} "
            f"--grpc=0.0.0.0:{worker['grpc_port']} "
            f"--connection={name}:{worker['connection_port']} "
            f"> /tmp/{name}.log 2>&1 &"
        )
        info(f"*** {name}: {cmd}\n")
        nodes[name].cmd(cmd)

    time.sleep(3)

    info("*** Last worker log lines\n")
    for worker in WORKERS:
        name = worker["name"]
        info(f"*** {name}\n")
        info(nodes[name].cmd(f"tail -n 20 /tmp/{name}.log || true"))

    info("*** Useful checks:\n")
    info("***   node1 ps aux\n")
    info("***   node1 tail -f /tmp/node1.log\n")
    info("***   node2 tail -f /tmp/node2.log\n")
    info("***   node3 tail -f /tmp/node3.log\n")
    info("*** Running CLI\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()
