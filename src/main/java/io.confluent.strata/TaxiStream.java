package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

        Serializer<Long> longSerializer = new LongSerializer();
        Deserializer<Long> longDeserializer = new LongDeserializer();

        KStream<GenericRecord, GenericRecord> taxiRides =
                builder.stream(genericRecordDeserializer, genericRecordDeserializer, "taxis_jdbc_yellow_cab_trips");

        /*
        KStream<GenericRecord, GenericRecord> weather =
                builder.stream(genericRecordDeserializer, genericRecordDeserializer, "taxis_jdbc_weather");

        KStream<Long, GenericRecord> keyedTaxiRides =
                taxiRides.map(new KeyValueMapper<GenericRecord, GenericRecord, KeyValue<Long,GenericRecord>>() {
                    @Override
                    public KeyValue<Long, GenericRecord> apply(GenericRecord key, GenericRecord value) {
                        Long time = ((Integer) value.get("tpep_pickup_datetime")).longValue();
                        if (time == null) {
                            System.err.printf("null time value in %s\n", value);
                            throw new RuntimeException("null ts in taxi data");
                        }
                        time = (time / 3600) * 3600;
                        return KeyValue.pair(time, value);
                    }
                }).through("keyedTaxiRides", longSerializer, genericRecordSerializer,
                        longDeserializer, genericRecordDeserializer);
        */

        /*
        KStream<Long, GenericRecord> keyedWeather =
                weather.map(new KeyValueMapper<GenericRecord, GenericRecord, KeyValue<Long,GenericRecord>>() {
                    @Override
                    public KeyValue<Long, GenericRecord> apply(GenericRecord key, GenericRecord value) {
                        Long time = ((Integer) value.get("ts")).longValue();
                        if (time == null) {
                            System.err.printf("null time value in %s\n", value);
                            throw new RuntimeException("null ts in weather data");
                        }
                        time = (time / 3600) * 3600;
                        return KeyValue.pair(time, value);
                    }
                }).through("keyedWeather", longSerializer, genericRecordSerializer,
                        longDeserializer, genericRecordDeserializer);
        */

        /*
        long oneYear = 365 * 24 * 60 * 60 * 1000L;

        KStream<Long, GenericRecord> joint =
                keyedTaxiRides.join(
                        keyedWeather,
                        new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>() {
                            RecordPairFactory recordPairFactory = null;
                            @Override
                            public GenericRecord apply(GenericRecord genericRecord, GenericRecord genericRecord2) {
                                if (recordPairFactory == null) {
                                    recordPairFactory = new RecordPairFactory(
                                            "withweather", "io.confluent.strata",
                                            genericRecord.getSchema(), genericRecord2.getSchema());
                                }
                                System.err.printf("%s %s\n", genericRecord, genericRecord2);
                                return recordPairFactory.of(genericRecord, genericRecord2);
                            }
                        },
                        (JoinWindows) JoinWindows.of("blah")
                                .within(3600000)
                                //.until(oneYear)
                        ,
                        longSerializer,
                        genericRecordSerializer,
                        genericRecordSerializer,
                        longDeserializer,
                        genericRecordDeserializer,
                        genericRecordDeserializer
                );

        joint.to("weatheredRides", new LongSerializer(), genericRecordSerializer);
        */

        List<String> neighborhoods = (List<String>) propertyFile.get("neighborhoods");

        KStream<GenericRecord, GenericRecord> geocodedRides =
                taxiRides.mapValues(
                        new ReverseGeocoder(neighborhoods, "pickup_latitude", "pickup_longitude", "neighborhood"));
        geocodedRides.to("geocodedRides", genericRecordSerializer, genericRecordSerializer);

        /*
        KStream<Long, GenericRecord> taxiRidesWithWeather = keyedTaxiRides.join(keyedWeather,
                new TaxiWeatherValueJoiner(),
                JoinWindows.of("stuff").within(3600),
                new LongSerializer(),
                genericRecordSerializer,
                genericRecordSerializer,
                new LongDeserializer(),
                genericRecordDeserializer,
                genericRecordDeserializer
                );
                */


        // keyedWeather.to("keyedweather", new LongSerializer(), genericRecordSerializer);

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
