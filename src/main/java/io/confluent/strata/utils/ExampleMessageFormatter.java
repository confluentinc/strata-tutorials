package io.confluent.strata.utils;

import com.google.common.collect.Maps;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.PrintStream;
import java.util.Properties;

/**
 * Created by jadler on 4/1/16.
 */
public class ExampleMessageFormatter implements kafka.tools.MessageFormatter {

    Deserializer<Object> keyDeserializer =null;
    Deserializer<Object> valueDeserializer =null;
    String topic = null;
    String keySeparator = null;
    String lineSeparator = null;

    static final String KEY_DESERIALIZER = "key.deserializer";
    static final String VALUE_DESERIALIZER = "value.deserializer";
    static final String TOPIC = "topic";
    static final String KEY_SEPARATOR = "key.separator";
    static final String LINE_SEPARATOR = "line.separator";

    public ExampleMessageFormatter() {}

    @Override
    public void writeTo(byte[] key, byte[] value, PrintStream output) {
        Object keyObject   = keyDeserializer.deserialize(topic, key);
        Object valueObject = valueDeserializer.deserialize(topic, value);
        output.append(keyObject.toString());
        output.append(keySeparator);
        output.append(valueObject.toString());
        output.append(lineSeparator);
        output.flush();
    }

    @Override
    public void init(Properties props) {
        try {
            topic = props.getProperty(TOPIC);
            keySeparator = props.containsKey(KEY_SEPARATOR) ? props.getProperty(KEY_SEPARATOR) : "\t";
            lineSeparator = props.containsKey(LINE_SEPARATOR) ? props.getProperty(LINE_SEPARATOR) : "\n";
            if (!props.containsKey(KEY_DESERIALIZER))
                throw new RuntimeException("Must specify " + KEY_DESERIALIZER);
            keyDeserializer = (Deserializer<Object>) Class.forName(props.getProperty(KEY_DESERIALIZER)).newInstance();
            keyDeserializer.configure(Maps.fromProperties(props), true);
            if (!props.containsKey(VALUE_DESERIALIZER))
                throw new RuntimeException("Must specify " + VALUE_DESERIALIZER);
            valueDeserializer = (Deserializer<Object>) Class.forName(props.getProperty(VALUE_DESERIALIZER)).newInstance();
            valueDeserializer.configure(Maps.fromProperties(props), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
