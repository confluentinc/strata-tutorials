package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
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
        SchemaRegistryClient schemaRegistryClient =
                new CachedSchemaRegistryClient(settings.getProperty("schema.registry.url"), 16);
        Deserializer<GenericRecord> ds = new GenericAvroDeserializer(schemaRegistryClient);
        KStream<GenericRecord, GenericRecord> taxiRides =
                builder.stream(ds, ds, "taxis_jdbc_yellow_cab_trips");

        // KStream<Object, GenericRecord> weather = builder.stream("taxis_jdbc_weather");

        List<String> neighborhoods = (List<String>) propertyFile.get("neighborhoods");

        // first, reverse-geocode!
        // to do: specify location of shapefile(s)
        KStream<String, GenericRecord> geocodedRides =
                taxiRides.mapValues(
                        new ReverseGeocoder(neighborhoods, "pickup_latitude", "pickup_longitude", "neighborhood"))
                .map(
                        new KeyValueMapper<GenericRecord, GenericRecord, KeyValue<String,GenericRecord>>() {
                            @Override
                            public KeyValue<String, GenericRecord> apply(GenericRecord o, GenericRecord genericRecord) {
                                // System.err.printf("key='%s',value='%s'\n",o,genericRecord);
                                return KeyValue.pair((o == null) ? "" : o.toString(), genericRecord);
                            }
                        });

        /*
        // now, join with weather data
        KStream<Long, GenericRecord> keyedTaxiRides =
                geocodedRides.map(new KeyValueMapper<Object, GenericRecord, KeyValue<Long,GenericRecord>>() {
                    @Override
                    public KeyValue<Long, GenericRecord> apply(Object o, GenericRecord genericRecord) {
                        Long time = (Long) genericRecord.get("tpep_pickup_datetime");
                        time = (time / 3600) * 3600;
                        return KeyValue.pair(time, genericRecord);
                    }
                });

        KStream<Long, GenericRecord> keyedWeather =
                weather.map(new KeyValueMapper<Object, GenericRecord, KeyValue<Long,GenericRecord>>() {
                    @Override
                    public KeyValue<Long, GenericRecord> apply(Object o, GenericRecord genericRecord) {
                        Long time = (Long) genericRecord.get("TS");
                        time = (time / 3600) * 3600;
                        return KeyValue.pair(time, genericRecord);
                    }
                });

        KStream<Long, GenericRecord> taxiRidesWithWeather = keyedTaxiRides.join(keyedWeather,
                new TaxiWeatherValueJoiner(),
                JoinWindows.of("stuff").within(3600),
                new LongSerializer(),
                new GenericAvroSerializer(),
                new GenericAvroSerializer(),
                new LongDeserializer(),
                new GenericAvroDeserializer(),
                new GenericAvroDeserializer()
                );
        */



        // taxiRidesWithWeather.to("taxi_rides_with_weather", new LongSerializer(), new GenericAvroSerializer());
        geocodedRides.to("geocoded_rides",
                new StringSerializer(),
                new GenericAvroSerializer(schemaRegistryClient));


        // chain in the config file
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace(System.err);
                System.err.print("uncaught exception " + e.toString());

            }
        });

    }
}
