#!/usr/bin/env python3
from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker

setLogLevel('info')

# Number of TaskManager slots (parallel sub-tasks) in the single TaskManager
NUM_TASK_MANAGER_SLOTS = 4

# Clean up any existing Flink container
def cleanup(name):
    c = docker.from_env()
    try:
        ctr = c.containers.get(name)
        info(f"*** Stopping & removing existing '{name}'\n")
        ctr.stop(); ctr.remove()
    except docker.errors.NotFound:
        pass

cleanup('flink')

# Create network
net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

# Add single Flink container to run both JobManager and TaskManager\ ninfo('*** Adding Flink container (JM + TM)\n')
flink = net.addDocker(
    name='flink',
    hostname='flink',
    ip='10.1.1.1',
    dimage='flink:1.17.2-scala_2.12',
    ports=[8081, 6123],
    port_bindings={8081: 8081, 6123: 6123},
    environment={
        'JOB_MANAGER_RPC_ADDRESS': 'flink',
        'TASK_MANAGER_NUMBER_OF_TASK_SLOTS': str(NUM_TASK_MANAGER_SLOTS),
        'FLINK_ENV_JAVA_OPTS': '-Xmx3g -Xms3g -XX:MaxMetaspaceSize=512m'
    }
)

# Attach to switch
sw = net.addSwitch('sw1')
net.addLink(flink, sw, cls=TCLink, delay='30ms', bw=1000)

# Launch both JobManager and TaskManager in same container
info('*** Launching JobManager & TaskManager in flink container\n')
# Use & to background JobManager before starting TaskManager
flink.cmd('bash -c "/docker-entrypoint.sh jobmanager & /docker-entrypoint.sh taskmanager"')

# Give them time to start
flink.cmd('sleep 10')

# Verify REST endpoint
jm_up = flink.cmd('bash -c "curl -sSf http://localhost:8081 && echo REST_OK || echo REST_FAIL"')
info(f" flink→localhost:8081 → {jm_up.strip()}\n")

info(f"*** Flink cluster is up: 1 JobManager + 1 TaskManager ({NUM_TASK_MANAGER_SLOTS} slots)\n")
info('*** Dropping to CLI\n')
CLI(net)

info('*** Stopping network\n')
net.stop()

