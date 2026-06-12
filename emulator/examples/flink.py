#!/usr/bin/env python3
from mininet.net import Containernet
from mininet.node import Controller, Docker
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker

setLogLevel('info')

def cleanup(names):
    c = docker.from_env()
    for n in names:
        try:
            ctr = c.containers.get(n)
            info(f"*** Stopping & removing existing '{n}'\n")
            ctr.stop(); ctr.remove()
        except docker.errors.NotFound:
            pass

# remove any stray flink container
cleanup(['flink'])

net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

info('*** Adding single Flink container\n')
flink = net.addDocker(
    name='flink',
    hostname='flink',
    ip='10.1.1.1',
    dimage='flink:1.17.2-scala_2.12',   # or your preferred version
    ports=[8081, 6123],                 # REST (8081) and RPC (6123)
    port_bindings={8081:8081, 6123:6123},
    #volumes=['/home/ttekdogan/Desktop/nes/git/containernet_new/containernet/config/flink/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml'],
    environment={
        # ensure JM RPC address is itself
        'JOB_MANAGER_RPC_ADDRESS':'flink',
        'FLINK_ENV_JAVA_OPTS':'-Xmx3g -Xms3g -XX:MaxMetaspaceSize=512m'
    },
)

info('*** Adding switch and links\n')
sw = net.addSwitch('sw1')
net.addLink(flink, sw, cls=TCLink, delay='30ms', bw=1000)

info('*** Starting network\n')
net.start()

info('*** Launching JobManager in background\n')
flink.cmd('bash -c "/docker-entrypoint.sh jobmanager &"')
flink.cmd('sleep 8')   # give JM time to bind its ports

# sanity‐check inside the flink container:
jm_up = flink.cmd('bash -c "curl -sSf http://localhost:8081 && echo REST_OK || echo REST_FAIL"')
info(f" flink→localhost:8081 →  {jm_up.strip()}\n")

info('*** Launching TaskManager in background\n')
flink.cmd('bash -c "/docker-entrypoint.sh taskmanager &"')
flink.cmd('sleep 5')

# sanity‐check RPC port
#rpc_up = flink.cmd('bash -c "nc -z localhost 6123 && echo RPC_OK || echo RPC_FAIL"')
#info(f" flink→localhost:6123 →  {rpc_up.strip()}\n")

# inside your existing single‐container setup, assuming 'flink' is your Flink‐container handle:
#flink.cmd('apt-get update && apt-get install -y mosquitto')
#flink.cmd('mosquitto -p 1882 -d')   # launches the broker as a daemon in the same container

info('*** Flink (JM+TM) is up on 10.1.1.1:{8081,6123}\n')
info('*** You can now add other containers (producers/consumers) attached to sw1\n')
info('*** Dropping to CLI\n')
CLI(net)

info('*** Stopping network\n')
net.stop()
