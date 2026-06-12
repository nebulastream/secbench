#!/usr/bin/env python3
from mininet.net import Containernet
from mininet.node import Controller
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import info, setLogLevel
import docker

setLogLevel('info')

# Number of TaskManager slots
NUM_TASK_MANAGER_SLOTS = 4

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'
KAFKA_HOST_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_IMAGE = 'apache/kafka:3.7.0'


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
cleanup('mn.kafka')

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
        'KAFKA_BOOTSTRAP_SERVERS': KAFKA_BOOTSTRAP_SERVERS,
    }
)

# Add Kafka broker container in KRaft mode (no ZooKeeper)
info('*** Adding Kafka broker container\n')
kafka = net.addDocker(
    name='kafka',
    hostname='kafka',
    ip='10.1.1.2',
    dimage=KAFKA_IMAGE,
    dcmd=['/__cacert_entrypoint.sh', '/etc/kafka/docker/run'],
    ports=[9092, 9094],
    port_bindings={9094: 9092},
    environment={
        'KAFKA_NODE_ID': '1',
        'KAFKA_PROCESS_ROLES': 'broker,controller',
        'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@kafka:9093',
        'KAFKA_LISTENERS': 'INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093',
        'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://{KAFKA_BOOTSTRAP_SERVERS},EXTERNAL://{KAFKA_HOST_BOOTSTRAP_SERVERS}',
        'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT',
        'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
        'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
        'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'true',
        'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
        'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
        'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
    }
)

# Create a switch and link containers
dsw = net.addSwitch('sw1')
net.addLink(flink, dsw, cls=TCLink, delay='10ms', bw=1000)
net.addLink(kafka, dsw, cls=TCLink, delay='10ms', bw=1000)

info('*** Starting network\n')
net.start()

# Start Flink processes via entrypoint
info('*** Starting Flink JobManager and TaskManager\n')
flink.cmd('/docker-entrypoint.sh jobmanager &')
flink.cmd('/docker-entrypoint.sh taskmanager &')

# Wait for services to initialize
flink.cmd('sleep 8')
kafka.cmd('for i in $(seq 1 30); do /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1 && break; sleep 1; done')

# Check Flink REST interface
status = flink.cmd('curl -sSf http://localhost:8081 && echo UP || echo DOWN').strip()
info(f"*** Flink REST API status: {status}\n")

# Check Kafka broker metadata from inside the Kafka container
kafka_status = kafka.cmd('/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka:9092 >/dev/null 2>&1 && echo UP || echo DOWN').strip()
info(f"*** Kafka broker status: {kafka_status}\n")

info(f"*** Flink running at flink:8081, Kafka internal bootstrap at {KAFKA_BOOTSTRAP_SERVERS}\n")
info(f"*** Host Kafka bootstrap at {KAFKA_HOST_BOOTSTRAP_SERVERS}\n")
info('*** Dropping to CLI\n')
CLI(net)

info('*** Stopping network\n')
net.stop()
