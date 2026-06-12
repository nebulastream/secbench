#!/usr/bin/env python3
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from mininet.net import Containernet
from mininet.node import Controller, Docker
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
from ipaddress import IPv4Address
import docker
import time

setLogLevel('debug')


def delete_containers_by_image(images):
    client = docker.from_env()
    containers = client.containers.list(all=True)
    for container in containers:
        tags = container.image.tags or []
        if any(img in tags[0] for img in images):
            info(f"Stopping and removing container {container.name} ({tags[0]})\n")
            container.stop()
            container.remove()
        else:
            info(f"Skipping container {container.name} ({tags[0] if tags else 'no-tag'})\n")


def remove_existing_containers(container_names):
    client = docker.from_env()
    for name in container_names:
        try:
            c = client.containers.get(name)
            info(f"Stopping and removing existing container: {name}\n")
            c.stop()
            c.remove()
        except docker.errors.NotFound:
            info(f"No existing container found with name: {name}\n")


def generate_container_names(number=1):
    names = [f"mn.w{i}" for i in range(1, number + 1)]
    names.append("mn.crd")
    return names


def generate_object_names(number=1):
    return [f"w{i}" for i in range(1, number + 1)]


if __name__ == "__main__":
    # Number of worker nodes to spin up
    numOfNodes = 2

    containerNames = generate_container_names(numOfNodes)
    workerNames = generate_object_names(numOfNodes)

    # Clean up any old containers
    delete_containers_by_image(containerNames)
    remove_existing_containers(containerNames)

    # Create the Containernet network with /24 mask for 10.1.1.0/24
    net = Containernet(controller=Controller, ipBase="10.1.1.0/24")
    info("*** Adding controller\n")
    net.addController("c0")

    info("*** Adding Docker containers\n")
    # Coordinator
    crd = net.addDocker(
        "crd",
        ip="10.1.1.1/24",
        prefixLen=24,
        dimage="nebulastream/nes-executable-image:latest",
        ports=[8081, 12346, 4000, 4001, 4002, 1200, 1216],
        port_bindings={8081: 8081, 12346: 12346, 4000: 4000, 4001: 4001, 4002: 4002},
        volumes=["/home/ttekdogan/Desktop/nes/git/containernet_new/cebench/config/nebulastream/conf1:/config"],
    )

    # Workers
    base_ip = IPv4Address("10.1.1.2")
    for idx, workerName in enumerate(workerNames):
        ip_addr = base_ip + idx
        ip_cidr = f"{ip_addr}/24"
        globals()[workerName] = net.addDocker(
            workerName,
            ip=ip_cidr,
            prefixLen=24,
            dimage="nebulastream/nes-executable-image:latest",
            ports=[3000, 3001],
            volumes=["/home/ttekdogan/Desktop/nes/git/containernet_new/cebench/config/nebulastream/conf1:/config"],
            depends_on=["crd"],
        )

    info("*** Adding switches\n")
    sw1 = net.addSwitch("sw1")

    info("*** Creating links\n")
    net.addLink(crd, sw1, cls=TCLink, delay="30ms")
    for workerName in workerNames:
        net.addLink(workerName, sw1, cls=TCLink, delay="10ms")

    info("*** Starting network\n")
    net.start()

    info("*** Launching NebulaStream processes inside containers\n")
    crd.cmd("nesCoordinator --configPath=/config/coordinator.yaml &")
    # give coordinator a moment to start
    time.sleep(10)
    for workerName in workerNames:
        globals()[workerName].cmd(f"nesWorker --configPath=/config/{workerName}.yaml &")

    info("*** Running CLI\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()
