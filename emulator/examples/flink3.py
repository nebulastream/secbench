#!/usr/bin/env python3
from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker
from pathlib import Path

setLogLevel('info')

REPO_ROOT = Path(__file__).resolve().parents[1]

# Number of TaskManager slots
NUM_TASK_MANAGER_SLOTS = 4

# Clean up existing containers by name
def cleanup(name):
    client = docker.from_env()
    try:
        ctr = client.containers.get(name)
        info(f"*** Stopping & removing existing '{name}' container...\n")
        ctr.stop()
        ctr.remove()
    except docker.errors.NotFound:
        pass

cleanup('mn.flink')
cleanup('mn.mqtt')

# Initialize Containernet
net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

# Add Flink container (JobManager + TaskManager)
info('*** Adding Flink container (JM + TM)\n')
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
        'FLINK_ENV_JAVA_OPTS': '-Xmx3g -Xms3g -XX:MaxMetaspaceSize=512m',
        'MQTT_BROKER_HOST': 'mqtt',
        'MQTT_BROKER_PORT': '1882'
    }
)

# Add MQTT broker container (Mosquitto)
info('*** Adding MQTT broker container (Mosquitto)\n')
mqtt = net.addDocker(
    name='mqtt',
    hostname='mqtt',
    ip='10.1.1.2',
    dimage='cebench-mosquitto:2.0-bash',
    build_params={
        'path': str(REPO_ROOT / 'mosquitto'),
        'tag': 'cebench-mosquitto:2.0-bash',
    },
    # Run Mosquitto directly
    dcmd=["/usr/sbin/mosquitto", "-c", "/mosquitto/config/mosquitto.conf"],
    volumes=[f'{REPO_ROOT / "config/mosquitto/mosquitto.conf"}:/mosquitto/config/mosquitto.conf'],
    ports=[1882, 9001],
    port_bindings={1882:1882, 9001:9002},
    detach=True
)

# Create a switch and link containers
dsw = net.addSwitch('sw1')
net.addLink(flink, dsw, cls=TCLink, delay='10ms', bw=1000)
net.addLink(mqtt, dsw, cls=TCLink, delay='10ms', bw=1000)

info('*** Starting network\n')
net.start()

# Start Flink processes via entrypoint
info('*** Starting Flink JobManager and TaskManager\n')
flink.cmd('/docker-entrypoint.sh jobmanager &')
flink.cmd('/docker-entrypoint.sh taskmanager &')

# Wait for Flink to initialize
flink.cmd('sleep 8')
# Check REST interface
status = flink.cmd('curl -sSf http://localhost:8081 && echo UP || echo DOWN').strip()
info(f"*** Flink REST API status: {status}\n")

info(f"*** Flink running at flink:8081, MQTT at mqtt:1882\n")
info('*** Dropping to CLI\n')
CLI(net)

info('*** Stopping network\n')
net.stop()
