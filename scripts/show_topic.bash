#!/usr/bin/env bash

export CLASSPATH=/opt/confluent/share/java/kafka-serde-tools/kafka-avro-serializer-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/kafka-schema-registry-client-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/confluent-common/common-config-2.1.0-alpha1.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/jackson-databind-2.5.4.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/jackson-core-2.5.4.jar
export CLASSPATH=$CLASSPATH:/opt/confluent/share/java/schema-registry/jackson-annotations-2.5.0.jar
kafka-console-consumer --zookeeper localhost:2181 --from-beginning \
 --value-deserializer io.confluent.kafka.serializers.KafkaAvroDeserializer \
 --key-deserializer io.confluent.kafka.serializers.KafkaAvroDeserializer \
 --formatter io.confluent.kafka.formatter.AvroMessageFormatter \
 --property schema.registry.url=http://localhost:8081 \
 --max-messages 50 \
 --print.key=true
 --topic $@
