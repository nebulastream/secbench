#!/usr/bin/env python3

"""Run a 64-worker NebulaStream deployment in Containernet.

The workers are arranged in four pods.  Each pod contains four edge
switches with four workers per edge switch.  A pod aggregation switch joins
the edge switches to a single core switch, to which the NebulaStream CLI
container is also connected.
"""

import os
import time

from mininet.net import Containernet
from mininet.node import Controller, OVSSwitch
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel

import dist_nes2 as base


WORKER_COUNT = 64
POD_COUNT = 4
EDGES_PER_POD = 4
WORKERS_PER_EDGE = 4

# These limits apply to every worker container.  They can be changed without
# editing the script, for example:
# NES_CONTAINER_CPUS=1 NES_CONTAINER_MEMORY=2g sudo -E python3 examples/nes-scaled.py
base.CONTAINER_CPUS = float(os.environ.get("NES_CONTAINER_CPUS", "0.5"))
base.CONTAINER_MEMORY = os.environ.get("NES_CONTAINER_MEMORY", "1g")

INSTALL_DEBUG_TOOLS = os.environ.get(
    "NES_INSTALL_DEBUG_TOOLS", "0"
).lower() in ("1", "true", "yes")
SHOW_NETWORK_STATE = os.environ.get(
    "NES_SHOW_NETWORK_STATE", "0"
).lower() in ("1", "true", "yes")


def make_workers(count=WORKER_COUNT):
    """Create worker definitions with unique IP and host-side ports."""
    return [
        {
            "name": f"node{idx}",
            "ip": f"10.1.1.{idx}/24",
            "grpc_port": 8080,
            "connection_port": 9090,
            "host_port_bindings": {
                8080: 18079 + idx,
                9090: 19089 + idx,
            },
        }
        for idx in range(1, count + 1)
    ]


def add_worker_containers(net, workers):
    info("*** Adding 64 worker containers\n")
    nodes = {}
    volumes = base.worker_volumes()
    for worker in workers:
        nodes[worker["name"]] = base.add_docker_with_resource_fallback(
            net,
            worker["name"],
            dimage=base.IMAGE,
            ports=[worker["grpc_port"], worker["connection_port"]],
            port_bindings=worker["host_port_bindings"],
            volumes=volumes,
            publish_all_ports=True,
            privileged=True,
        )
    return nodes


def add_nebuli_container(net, nodes):
    info("*** Adding nebuli container\n")
    nodes[base.NEBULI["name"]] = base.add_docker_with_resource_fallback(
        net,
        base.NEBULI["name"],
        dimage=base.NEBULI_IMAGE,
        dcmd="bash -lc 'trap : TERM INT; while true; do sleep 3600; done'",
        volumes=base.nebuli_volumes(),
        privileged=True,
    )


def add_scaled_tree_topology(net, nodes, workers):
    """Build a core/aggregation/edge tree with four workers per edge."""
    info("*** Adding scaled three-tier tree topology\n")
    core = net.addSwitch("s1", cls=OVSSwitch, failMode="standalone")
    aggregations = [
        net.addSwitch(f"s{idx}", cls=OVSSwitch, failMode="standalone")
        for idx in range(2, 2 + POD_COUNT)
    ]
    edge_switch_start = 2 + POD_COUNT
    edges = [
        net.addSwitch(
            f"s{edge_switch_start + idx}",
            cls=OVSSwitch,
            failMode="standalone",
        )
        for idx in range(POD_COUNT * EDGES_PER_POD)
    ]

    for pod_idx, aggregation in enumerate(aggregations):
        net.addLink(core, aggregation, cls=TCLink, bw=2000, delay="8ms")
        for local_edge_idx in range(EDGES_PER_POD):
            edge_idx = pod_idx * EDGES_PER_POD + local_edge_idx
            edge = edges[edge_idx]
            net.addLink(aggregation, edge, cls=TCLink, bw=1000, delay="3ms")

            first_worker = edge_idx * WORKERS_PER_EDGE
            for worker in workers[
                first_worker:first_worker + WORKERS_PER_EDGE
            ]:
                net.addLink(
                    nodes[worker["name"]],
                    edge,
                    cls=TCLink,
                    bw=100,
                    delay="1ms",
                )

    net.addLink(nodes[base.NEBULI["name"]], core, cls=TCLink, bw=1000, delay="1ms")


def show_sample_worker_logs(nodes):
    """Show representative logs without printing output from all 64 nodes."""
    info("*** Sample worker log lines\n")
    for name in ("node1", "node32", "node64"):
        info(f"*** {name}\n")
        info(nodes[name].cmd(f"tail -n 20 /tmp/{name}.log || true"))


def main():
    if WORKER_COUNT != POD_COUNT * EDGES_PER_POD * WORKERS_PER_EDGE:
        raise ValueError("The topology dimensions must describe exactly 64 workers")

    base.WORKERS = make_workers()
    base.NEBULI = {"name": "nebuli", "ip": "10.1.1.254/24"}
    base.ALL_NODES = base.WORKERS + [base.NEBULI]

    base.remove_existing_containers(
        [entry["name"] for entry in base.ALL_NODES]
    )

    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")
    info("*** Adding controller\n")
    net.addController("c0")

    base.log_container_resource_limits()
    nodes = add_worker_containers(net, base.WORKERS)
    add_nebuli_container(net, nodes)
    add_scaled_tree_topology(net, nodes, base.WORKERS)

    try:
        info("*** Starting network\n")
        net.start()
        time.sleep(2)

        if INSTALL_DEBUG_TOOLS:
            base.install_debug_tools(nodes)
        else:
            info("*** Skipping debug-tool installation (set NES_INSTALL_DEBUG_TOOLS=1 to enable)\n")

        base.configure_mininet_ips(nodes)
        base.configure_hosts_file(nodes)
        base.sync_worker_models(nodes)
        if SHOW_NETWORK_STATE:
            base.show_network_state(nodes)
        base.start_workers(nodes)

        time.sleep(3)
        show_sample_worker_logs(nodes)

        info("*** Deployment: 64 workers, 4 pods, 16 edge switches\n")
        info("*** Worker limits: 0.5 CPU and 1 GiB RAM by default\n")
        info("*** Useful checks:\n")
        info("***   nebuli ping -c 3 node1\n")
        info("***   nebuli ping -c 3 node64\n")
        info("***   nebuli getent hosts node64\n")
        info("***   sh docker exec -it mn.nebuli bash\n")
        info("***   inside nebuli: ls /queries\n")
        info("***   inside nebuli: nes-cli -d -t /queries/topology.yaml register -x -i /queries/query.yaml\n")
        info("*** Running CLI\n")
        CLI(net)
    finally:
        info("*** Stopping network\n")
        net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    main()
