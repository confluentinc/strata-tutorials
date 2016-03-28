package io.confluent.strata.utils;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * Created by jadler on 3/27/16.
 */
public class LongAsStringSerializer implements Serializer<Long> {

    private static final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Long data) {
        return stringSerializer.serialize(topic, data.toString());
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
