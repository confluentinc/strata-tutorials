package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.strata.geo.ReverseGeocoder;
import io.confluent.strata.utils.GenericAvroDeserializer;
import io.confluent.strata.utils.GenericAvroSerializer;
import io.confluent.strata.utils.WindowedStringSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jadler on 3/20/16.
 */
public class TaxiStream {

    public static void main(String args[]) throws IOException {

        // Load the settings from an external JSON file. (This is straightforward using Jackson.)
        // We like to keep settings in a separate file from the code. This makes it easy to move
        // your code between systems, or tweak settings in production.)
        ObjectMapper mapper = new ObjectMapper();
        System.err.printf("reading from file %s\n", args[0]);
        Map<String, Object> propertyFile =
                mapper.readValue(
                        new File(args[0]), new TypeReference<Map<String, Object>>() {
                        });
        Properties settings = new Properties();
        for (Map.Entry<String, Object> property : propertyFile.entrySet()) {
            settings.setProperty(property.getKey(), property.getValue().toString());
        }

        // Define and configure serializers and deserializers
        final Deserializer<GenericRecord> genericRecordDeserializer = new GenericAvroDeserializer();
        genericRecordDeserializer.configure(propertyFile, true);
        final Serializer<GenericRecord> genericRecordSerializer = new GenericAvroSerializer();
        genericRecordSerializer.configure(propertyFile, true);
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Serializer<Windowed<String>> windowedStringSerializer = new WindowedStringSerializer();

        // 1. Create (or load) the streams configuration


        // 2. Create a KStreamBuilder object


        // 3. Tell your streams job where to consume data from

        // 4. Transform the data
        List<String> neighborhoods = (List<String>) propertyFile.get("neighborhoods");

        // 9. Count messages by key
        final long oneDay = 24 * 60 * 60 * 1000;

        // 10. Write the results out to another stream.

        // 6. Start the streams job

        // Set a handler for uncaught exceptions within the framework:
        // uncomment this section if the KStreams object is called "streams", or rename then uncomment
        /*
        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace(System.err);
                System.err.printf("uncaught exception in thread %s: %s\n", t.toString(), e.toString());
                System.err.flush();
                System.exit(-1);
            }
        });
        */

    }
}