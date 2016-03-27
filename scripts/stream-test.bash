#!/usr/bin/env bash

rm -rf /tmp/kafka-streams

kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic geocodedRides
kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic movingAvgDistance
kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic countsByDay

java -cp /home/vagrant/target/strata-tutorial-0.1-SNAPSHOT-jar-with-dependencies.jar \
 -Dlog4j.debug io.confluent.strata.TaxiStream /home/vagrant/etc/stream.json