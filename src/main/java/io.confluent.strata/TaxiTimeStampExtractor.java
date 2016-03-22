package io.confluent.strata;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Created by jadler on 3/20/16.
 */
public class TaxiTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord) {
        GenericRecord value = (GenericRecord) consumerRecord.value();
        return ((Integer) value.get("tpep_pickup_datetime")).longValue();
    }
}
