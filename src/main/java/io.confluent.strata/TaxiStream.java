package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.confluent.examples.streams.utils.GenericAvroDeserializer;
import io.confluent.examples.streams.utils.GenericAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by jadler on 3/20/16.
 */
public class TaxiStream {

    public void main(String args[]) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        System.err.printf("reading from file %s\n", args[0]);
        Map<String,Object> propertyFile= mapper.readValue(new File(args[0]),
                new TypeReference<Map<String,Map<String,Object>>>() {});
        Properties settings = new Properties();
        for (Map.Entry<String,Object> property: propertyFile.entrySet()) {
            settings.setProperty(property.getKey(), property.getValue().toString());
        }

        StreamsConfig config = new StreamsConfig(settings);

        KStreamBuilder builder = new KStreamBuilder();
        KStream<Object, GenericRecord> taxiRides = builder.stream("taxis_jdbc_yellow_cab_trips");
        KStream<Object, GenericRecord> weather = builder.stream("taxis_jdbc_weather");


        // first, reverse-geocode!
        // to do: specify location of shapefile(s)
        KStream<Object, GenericRecord> geocodedRides =
                taxiRides.mapValues(new ReverseGeocoder(Lists.newArrayList()));

        // now, join with weather data
        KStream<Long, GenericRecord> keyedTaxiRides =
                taxiRides.map(new KeyValueMapper<Object, GenericRecord, KeyValue<Long,GenericRecord>>() {
                    @Override
                    public KeyValue<Long, GenericRecord> apply(Object o, GenericRecord genericRecord) {
                        Long time = (Long) genericRecord.get("tpep_pickup_datetime");
                        time = (time / 3600) * 3600;
                        return KeyValue.pair(time, genericRecord);
                    }
                });

        KStream<Long, GenericRecord> keyedWeather =
                taxiRides.map(new KeyValueMapper<Object, GenericRecord, KeyValue<Long,GenericRecord>>() {
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


        taxiRidesWithWeather.to("taxi_rides_with_weather");


        // chain in the config file
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
    }
}
