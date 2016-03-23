#!/usr/bin/env bash

#rm -rf /tmp/kafka-streams

#kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic keyedTaxiRides
#kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic keyedWeather
#kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic weatheredRides
#kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic geocodedRides

java -cp /vagrant/target/strata-tutorial-0.1-SNAPSHOT-jar-with-dependencies.jar \
 io.confluent.strata.TaxiStream /vagrant/etc/stream.json