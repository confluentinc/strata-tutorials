package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.strata.geo.ReverseGeocoder;
import io.confluent.strata.utils.GenericAvroDeserializer;
import io.confluent.strata.utils.GenericAvroSerializer;
import io.confluent.strata.utils.WindowedStringDeserializer;
import io.confluent.strata.utils.WindowedStringSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
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

        Deserializer<String> stringDeserializer = new StringDeserializer();
        Serializer<String> stringSerializer = new StringSerializer();

        Deserializer<Long> longDeserializer = new LongDeserializer();
        Serializer<Long> longSerializer = new LongSerializer();

        Deserializer<Windowed<String>> windowedStringDeserializer = new WindowedStringDeserializer();
        Serializer<Windowed<String>> windowedStringSerializer = new WindowedStringSerializer();

        // Load in the raw data
        KStream<GenericRecord, GenericRecord> taxiRides =
                builder.stream(genericRecordDeserializer, genericRecordDeserializer, "taxis_jdbc_yellow_cab_trips");

        // Reverse-geocode the pickup location and write out to a kafka topic as an intermediate step
        List<String> neighborhoods = (List<String>) propertyFile.get("neighborhoods");
        KStream<String, GenericRecord> geocodedRides = taxiRides
                .map(new ReverseGeocoder(neighborhoods, "pickup_latitude", "pickup_longitude", "neighborhood"))
                .through("geocodedRides",
                    stringSerializer, genericRecordSerializer,
                    stringDeserializer, genericRecordDeserializer);

        // Now compute some metrics
        final long oneDay = 24 * 60 * 60 * 1000;
        KTable<Windowed<String>, Long> cabRidesPerDay = geocodedRides.countByKey(
                HoppingWindows.of("days").every(oneDay).until(oneDay*365),
                stringSerializer, longSerializer,
                stringDeserializer, longDeserializer);

        cabRidesPerDay.to("countsByDay",windowedStringSerializer,longSerializer);

        // schema for average calculation
        final Schema metricAggSchema = SchemaBuilder
                .record("metricAgg")
                .fields()
                .requiredLong("count")
                .requiredDouble("total")
                .endRecord();

        final long oneHour = 60 * 60 * 1000;

        KTable<Windowed<String>, GenericRecord> movingAverageDistance = geocodedRides.aggregateByKey(
                new Initializer<GenericRecord>() {
                    @Override
                    public GenericRecord apply() {
                        GenericRecord newRecord = (GenericRecord) GenericData.get().newRecord(null, metricAggSchema);
                        newRecord.put("count", 0L);
                        newRecord.put("total", 0.0);
                        return newRecord;
                    }
                },
                new Aggregator<String, GenericRecord, GenericRecord>() {
                    @Override
                    public GenericRecord apply(String aggKey, GenericRecord value, GenericRecord aggregate) {
                        Double distanceFromRecord = (Double) value.get("trip_distance");
                        Long count = (Long) aggregate.get("count");
                        Double total = (Double) aggregate.get("total");
                        count += 1;
                        total += distanceFromRecord;
                        aggregate.put("count", count);
                        aggregate.put("total", total);
                        return aggregate;
                    }
                },
                TumblingWindows.of("movingHour").with(oneHour).until(oneDay*365),
                stringSerializer, genericRecordSerializer, stringDeserializer, genericRecordDeserializer);

        movingAverageDistance.to("movingAvgDistance",windowedStringSerializer,genericRecordSerializer);


        // chain in the config file
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace(System.err);
                System.err.printf("uncaught exception in thread %s: %s\n", t.toString(), e.toString());
                System.err.flush();
                System.exit(-1);
            }
        });

    }
}
