package io.confluent.strata.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Map;

/**
 * Created by jadler on 3/23/16.
 */
public class WindowedStringDeserializer implements Deserializer<Windowed<String>> {

    private static final StringDeserializer stringDeserializer = new StringDeserializer();

    public class SimpleWindow extends Window {
        public SimpleWindow(long start, long end) {
            super(start, end);
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        stringDeserializer.configure(configs, isKey);
    }

    @Override
    public Windowed<String> deserialize(String topic, byte[] data) {
        String deserializedString = stringDeserializer.deserialize(topic, data);
        String[] strings = deserializedString.split(",", 3);
        String value = strings[0];
        Window window = new SimpleWindow(Long.parseLong(strings[1]), Long.parseLong(strings[2]));
        return new Windowed<>(value, window);
    }

    @Override
    public void close() {
        stringDeserializer.close();
    }
}
