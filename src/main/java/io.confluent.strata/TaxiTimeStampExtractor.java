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
        Integer fromTaxiData = (Integer) value.get("tpep_pickup_datetime");
        if (fromTaxiData != null)
            return fromTaxiData.longValue() * 1000L;
        Integer fromWeatherData = (Integer) value.get("ts");
        if (fromWeatherData != null)
            return fromWeatherData * 1000L;
        System.err.printf("issue while extracting timestamp from %s\n", consumerRecord);
        return System.currentTimeMillis();
    }
}
