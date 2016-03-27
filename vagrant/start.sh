#!/bin/bash
set -e

# Starting Hadoop
echo "Starting Hadoop"
hadoop namenode -format -force -nonInteractive
hadoop-daemon.sh start namenode
# Why is this line repeated?
hadoop-daemon.sh start datanode

echo "Starting Zookeeper"
export KAFKA_HEAP_OPTS="-Xmx128M -Xms128M"
zookeeper-server-start /mnt/etc/zookeeper.properties 1>> /mnt/logs/zk.log 2>>/mnt/logs/zk.log &
sleep 5

echo "Starting Kafka"
kafka-server-start /mnt/etc/server.properties 1>> /mnt/logs/kafka.log 2>> /mnt/logs/kafka.log &
sleep 5

echo "Starting Schema Registry"
schema-registry-start /mnt/etc/schema-registry.properties 1>> /mnt/logs/schema-registry.log 2>> /mnt/logs/schema-registry.log &

# Starting Hive Metastore
echo "Not starting Hive metastore. Run start_hive.sh if you want it."
# hive --service metastore 1>> /mnt/logs/metastore.log 2>> /mnt/logs/metastore.log &
