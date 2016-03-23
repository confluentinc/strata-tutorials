package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.strata.geo.ReverseGeocoder;
import io.confluent.strata.utils.GenericAvroDeserializer;
import io.confluent.strata.utils.GenericAvroSerializer;
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

        ObjectMapper mapper = new ObjectMapper();
        System.err.printf("reading from file %s\n", args[0]);
        Map<String,Object> propertyFile= mapper.readValue(new File(args[0]),
                new TypeReference<Map<String,Object>>() {});
        Properties settings = new Properties();
        for (Map.Entry<String,Object> property: propertyFile.entrySet()) {
            settings.setProperty(property.getKey(), property.getValue().toString());
        }

        StreamsConfig config = new StreamsConfig(settings);

        KStreamBuilder builder = new KStreamBuilder();

        Deserializer<GenericRecord> genericRecordDeserializer = new GenericAvroDeserializer();
        genericRecordDeserializer.configure(propertyFile, true);
        Serializer<GenericRecord> genericRecordSerializer = new GenericAvroSerializer();
        genericRecordSerializer.configure(propertyFile, true);

        KStream<GenericRecord, GenericRecord> taxiRides =
                builder.stream(genericRecordDeserializer, genericRecordDeserializer, "taxis_jdbc_yellow_cab_trips");

        List<String> neighborhoods = (List<String>) propertyFile.get("neighborhoods");

        KStream<GenericRecord, GenericRecord> geocodedRides =
                taxiRides.mapValues(
                        new ReverseGeocoder(neighborhoods, "pickup_latitude", "pickup_longitude", "neighborhood"));
        geocodedRides.to("geocodedRides", genericRecordSerializer, genericRecordSerializer);


        // chain in the config file
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace(System.err);
                System.err.printf("uncaught exception in thread %s: %s\n", t.toString(), e.toString());
                //System.exit(-1);
            }
        });

    }
}
