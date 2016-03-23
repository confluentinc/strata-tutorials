package io.confluent.strata.utils;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Created by jadler on 3/23/16.
 *
 * workaround for when the key is null
 *
 */
public class ConstantDeserializer implements Deserializer<Integer> {

    Integer _whatever = 0;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Integer deserialize(String topic, byte[] data) {
        return _whatever;
    }

    @Override
    public void close() {

    }
}
