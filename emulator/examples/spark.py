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

# remove any stray Spark container
cleanup(['mn.spark'])

net = Containernet(controller=Controller)
info('*** Adding controller\n')
net.addController('c0')

info('*** Adding single Spark container\n')
spark = net.addDocker(
    name='spark',
    hostname='spark',
    ip='10.1.1.1',
    dimage='apache/spark:3.4.0',
    ports=[8080, 7077],           # Web UI & master RPC
    port_bindings={8080:8080, 7077:7077},
    volumes=[
        # mount your Structured Streaming job
        '/home/you/spark-mqtt/jobs:/opt/spark/jobs',
        # inject the MQTT connector
        '/home/you/spark-mqtt/jars/spark-sql-streaming-mqtt_2.12-3.0.0.jar'
        ':/opt/spark/jars/spark-sql-streaming-mqtt_2.12-3.0.0.jar',
    ],
    environment={
        'SPARK_MASTER_HOST': 'spark'
    },
)

info('*** Adding switch and links\n')
sw = net.addSwitch('sw1')
net.addLink(spark, sw, cls=TCLink, delay='30ms', bw=1000)

info('*** Starting network\n')
net.start()

info('*** Launching Spark Master in background\n')
spark.cmd('bash -c "/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master '
          '--host spark --port 7077 &"')
spark.cmd('sleep 8')   # give master time

info('*** Launching Spark Worker in background\n')
spark.cmd('bash -c "/opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker '
          'spark://spark:7077 &"')
spark.cmd('sleep 5')

info('*** Spark (Master+Worker) is up on 10.1.1.1:{8080,7077}\n')
info('*** Drop into CLI to submit your Streaming job\n')
CLI(net)

info('*** Stopping network\n')
net.stop()

