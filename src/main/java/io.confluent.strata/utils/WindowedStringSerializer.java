package io.confluent.strata.utils;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Map;

/**
 * Created by jadler on 3/23/16.
 */
public class WindowedStringSerializer implements Serializer<Windowed<String>> {

    private static final StringSerializer stringSerializer = new StringSerializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Windowed<String> data) {
        StringBuilder dataString = new StringBuilder();
        dataString.append(data.value());
        dataString.append(",");
        dataString.append(data.window().start());
        dataString.append(",");
        dataString.append(data.window().end());
        return stringSerializer.serialize(topic, dataString.toString());
    }

    @Override
    public void close() {
        stringSerializer.close();
    }
}
