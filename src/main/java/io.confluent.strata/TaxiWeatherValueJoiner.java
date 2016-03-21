package io.confluent.strata;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 * Created by jadler on 3/21/16.
 */
public class TaxiWeatherValueJoiner implements ValueJoiner<GenericRecord, GenericRecord, GenericRecord> {

    @Override
    public GenericRecord apply(GenericRecord genericRecord1, GenericRecord genericRecord2) {
        return null;
    }
}
