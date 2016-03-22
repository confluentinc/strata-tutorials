#!/usr/bin/env bash

java -cp /vagrant/target/strata-tutorial-0.1-SNAPSHOT-jar-with-dependencies.jar \
 io.confluent.strata.TaxiStream /vagrant/etc/stream.json