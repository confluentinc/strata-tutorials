package io.confluent.strata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.strata.geo.ReverseGeocoder;
import io.confluent.strata.utils.GenericAvroDeserializer;
import io.confluent.strata.utils.GenericAvroSerializer;
import io.confluent.strata.utils.LongAsStringSerializer;
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
 * A fully working version of the TaxiStream application, with all code completed, and
 * explicit comments.
 *
 * (Do you have suggestions how to make this more elegant? Send them to us!)
 *
 */
public class TaxiStreamSolution {

    public static void main(String args[]) throws IOException {

        // Load the settings from an external JSON file. (This is straightforward using Jackson.)
        // We like to keep settings in a separate file from the code. This makes it easy to move
        // your code between systems, or tweak settings in production.)
        ObjectMapper mapper = new ObjectMapper();
        System.err.printf("reading from file %s\n", args[0]);
        Map<String,Object> propertyFile =
                mapper.readValue(
                        new File(args[0]), new TypeReference<Map<String,Object>>() {});
        Properties settings = new Properties();
        for (Map.Entry<String,Object> property: propertyFile.entrySet()) {
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
        final Serializer<Long> longAsStringSerializer = new LongAsStringSerializer();
        final Serializer<Windowed<String>> windowedStringSerializer = new WindowedStringSerializer();

        // Create (or load) the streams configuration
        StreamsConfig config = new StreamsConfig(settings);


        // Create a KStreamBuilder object
        KStreamBuilder builder = new KStreamBuilder();

        // Tell your streams job where to consume data from
        KStream<GenericRecord, GenericRecord> taxiRides =
                builder.stream(genericRecordDeserializer, genericRecordDeserializer, "taxis_jdbc_yellow_cab_trips");

        // Transform the data
        List<String> neighborhoods = (List<String>) propertyFile.get("neighborhoods");
        KStream<String, GenericRecord> geocodedRides = taxiRides
                .map(new ReverseGeocoder(neighborhoods, "pickup_latitude", "pickup_longitude", "neighborhood"))
                // Write the data out to another Kafka topic
                // Tell Kafka Streams that you want to write the stream out to Kafka, but still use it.
                .through("geocodedRides",
                    stringSerializer, genericRecordSerializer,
                    stringDeserializer, genericRecordDeserializer);

        // Count messages by key
        final long oneDay = 24 * 60 * 60 * 1000;
        KTable cabRidesPerDay = geocodedRides.countByKey(
                HoppingWindows.of("days").every(oneDay).until(oneDay*365),
                stringSerializer, longSerializer,
                stringDeserializer, longDeserializer);

        // Write the results out to another stream.
        cabRidesPerDay.to("countsByDay",windowedStringSerializer,longAsStringSerializer);

        // A third example for the adventurous. This time, we'll calculate a moving average.
        // First, define a schema for computing averages (a count and a total):
        final Schema metricAggSchema = SchemaBuilder
                .record("metricAgg")
                .fields()
                .requiredLong("count")
                .requiredDouble("total")
                .endRecord();

        // now aggregate records together
        final long oneHour = 60 * 60 * 1000;
        KTable movingAverageDistance = geocodedRides.aggregateByKey(
                new Initializer<GenericRecord>() {
                    // at the beginning, both counters are zero
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
                        // increment count, accumulate total distacce
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


        // Start the streams job
        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        // Set a handler for uncaught exceptions within the framework:
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
