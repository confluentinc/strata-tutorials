#!/usr/bin/env bash

# explicitly create topics needed by streams job

kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic keyedTaxiRides
kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic keyedWeather
kafka-topics --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic weatheredRides
