#!/usr/bin/env bash

export CLASSPATH=/opt/confluent/share/java/kafka-serde-tools/kafka-avro-serializer-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/kafka-schema-registry-client-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/confluent-common/common-config-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/confluent-common/common-utils-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/jackson-databind-2.5.4.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/jackson-core-2.5.4.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/jackson-annotations-2.5.0.jar
export CLASSPATH=$CLASSPATH:/vagrant/target/strata-tutorial-0.1-SNAPSHOT.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/kafka-connect-hdfs/guava-14.0.1.jar
kafka-console-consumer --zookeeper localhost:2181 --from-beginning \
 --property value.deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer \
 --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 --formatter io.confluent.strata.utils.BetterDefaultMessageFormatter \
 --property schema.registry.url=http://localhost:8081 \
 --property print.key=true \
 --max-messages 50 \
 --topic geocodedRides