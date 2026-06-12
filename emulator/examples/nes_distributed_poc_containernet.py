#!/usr/bin/env python3

from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker

setLogLevel('info')

IMAGE = "nebulastream/worker:distributed-poc"
WORKERS = [
    {
        "name": "node1",
        "ip": "10.1.1.1/24",
        "connection": "10.1.1.1:9090",
        "host_grpc": 8080,
        "host_data": 9090,
    },
    {
        "name": "node2",
        "ip": "10.1.1.2/24",
        "connection": "10.1.1.2:9090",
        "host_grpc": 8081,
        "host_data": 9091,
    },
    {
        "name": "node3",
        "ip": "10.1.1.3/24",
        "connection": "10.1.1.3:9090",
        "host_grpc": 8082,
        "host_data": 9092,
    },
]


def remove_existing_containers(container_names):
    client = docker.from_env()
    for name in container_names:
        try:
            container = client.containers.get(name)
            info(f"*** Stopping and removing existing container: {name}\n")
            container.stop()
            container.remove(force=True)
        except docker.errors.NotFound:
            pass


if __name__ == "__main__":
    # Containernet prefixes Docker containers with "mn.".
    remove_existing_containers([f"mn.{worker['name']}" for worker in WORKERS])

    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")

    info("*** Adding controller\n")
    net.addController("c0")

    info("*** Adding worker containers\n")
    nodes = {}
    for worker in WORKERS:
        # The distributed-poc image already defines the worker entrypoint.
        # Therefore, we only pass the arguments here, similar to docker-compose.
        dcmd = (
            f"--grpc=0.0.0.0:8080 "
            f"--connection={worker['connection']}"
        )

        nodes[worker["name"]] = net.addDocker(
            worker["name"],
            ip=worker["ip"],
            dimage=IMAGE,
            dcmd=dcmd,
            ports=[8080, 9090],
            port_bindings={
                worker["host_grpc"]: 8080,
                worker["host_data"]: 9090,
            },
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

    info("*** Worker overview\n")
    for worker in WORKERS:
        info(
            f"  - {worker['name']}: {worker['ip']} "
            f"grpc=localhost:{worker['host_grpc']} "
            f"connection={worker['connection']}\n"
        )

    info("*** Useful checks\n")
    info("  docker ps | grep distributed-poc\n")
    info("  sudo docker logs -f mn.node1\n")
    info("  sudo docker logs -f mn.node2\n")
    info("  sudo docker logs -f mn.node3\n")
    info("\n")

    CLI(net)

    info("*** Stopping network\n")
    net.stop()
