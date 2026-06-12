#!/usr/bin/env python
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/python

from mininet.net import Containernet
from mininet.node import Controller, Docker
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker
setLogLevel('debug')

def delete_containers_by_image(images):
    # Create a Docker client
    client = docker.from_env()

    # List all containers
    containers = client.containers.list(all=True)

    # Iterate through the list of containers
    for container in containers:
        # Get the image name of the container
        container_image = container.image.tags[0] if container.image.tags else None

        if container_image:
            # Check if the container's image matches any in the provided list
            if any(image in container_image for image in images):
                print(f"Stopping and removing container {container.name} with image {container_image}")
                container.stop()
                container.remove()
            else:
                print(f"Skipping container {container.name} with image {container_image}")
        else:
            print(f"Skipping container {container.name} with no tagged image")

def remove_existing_containers(container_names):
    client = docker.from_env()
    for name in container_names:
        try:
            container = client.containers.get(name)
            print(f"Stopping and removing existing container: {name}")
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            print(f"No existing container found with name: {name}")

delete_containers_by_image(['mn.crd', 'mn.w1', 'mn.w2', 'mn.w3'])
remove_existing_containers(['mn.crd', 'mn.w1', 'mn.w2', 'mn.w3'])

net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

info('*** Adding docker containers\n')

crd = net.addDocker('crd', ip='10.1.1.1',
                            dimage="nebulastream/nes-executable-image:latest",
                            ports=[8081, 12346, 4000, 4001, 4002, 1200, 1216],
                            port_bindings={8081:8081, 12346:12346, 4000:4000, 4001:4001, 4002:4002},
                            volumes=['/home/ttekdogan/Desktop/nes/git/containernet_new/cebench/config/nebulastream/conf1:/config'])

w1 = net.addDocker('w1', ip='10.1.1.2',
                            dimage="nebulastream/nes-executable-image:latest",
                            ports=[3000, 3001, 4005],
                            port_bindings={3000:3000, 3001:3001, 4005:4005},
                            volumes=['/home/ttekdogan/Desktop/nes/git/containernet_new/cebench/config/nebulastream/conf1:/config'],
                            depends_on=['crd'],
                            cpus=10,
                            memory="4g")

w2 = net.addDocker('w2', ip='10.1.1.3',
                            dimage="nebulastream/nes-executable-image:latest",
                            ports=[3000, 3001],
                            port_bindings={3002:3000, 3003:3001},
                            volumes=['/home/ttekdogan/Desktop/nes/git/containernet_new/cebench/config/nebulastream/conf1:/config'],
                            depends_on=['crd'])

w3 = net.addDocker('w3', ip='10.1.1.4',
                            dimage="nebulastream/nes-executable-image:latest",
                            ports=[3000, 3001],
                            port_bindings={3004:3000, 3005:3001},
                            volumes=['/home/ttekdogan/Desktop/nes/git/containernet_new/cebench/config/nebulastream/conf1:/config'],
                            depends_on=['crd'])


# install ping utility for test purposes
crd.cmd('apt-get update && apt-get install -y iputils-ping')
#w1.cmd('apt-get update && apt-get install -y iputils-ping')
#w2.cmd('apt-get update && apt-get install -y iputils-ping')
#w3.cmd('apt-get update && apt-get install -y iputils-ping')


info('*** Adding switches\n')
sw1 = net.addSwitch('sw1')

info('*** Creating links\n')
net.addLink(crd, sw1, cls=TCLink)
#net.addLink(w1, sw1, cls=TCLink, bw=10, delay='30ms')
net.addLink(w1, sw1, cls=TCLink)
net.addLink(w2, sw1, cls=TCLink)
net.addLink(w3, sw1, cls=TCLink)


info('*** Starting network\n')
net.start()

#crd.cmd("./entrypoint.sh &")
crd.cmd('nesCoordinator --configPath=/config/coordinator.yaml &')
crd.cmd('sleep 5 &')
w1.cmd('nesWorker --configPath=/config/producers-worker.yaml &')
#w2.cmd('nesWorker --configPath=/config/consumers-worker-1.yaml &')
#w3.cmd('nesWorker --configPath=/config/consumers-worker-2.yaml &')

'''
w2.cmd('/entrypoint.sh nesWorker --coordinatorPort=4000 --coordinatorIp=10.1.1.1 --localWorkerIp=10.1.1.3 --sourceType=CSVSource --sourceConfig=/home/ttekdogan/Desktop/nes/git/containernet/config/nebulastream/consumers-worker-1.yaml --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=streetLights --logicalStreamName=consumers')

w3.cmd('/entrypoint-containernet.sh nesWorker --coordinatorPort=4000 --coordinatorIp=10.1.1.1 --localWorkerIp=10.1.1.4 --sourceType=CSVSource --sourceConfig=/opt/local/nebula-stream/exdra.csv --numberOfBuffersToProduce=100 --sourceFrequency=1 --physicalStreamName=test_stream --logicalStreamName=exdra')
'''




info('*** Running CLI\n')
CLI(net)

info('*** Stopping network')
net.stop()
