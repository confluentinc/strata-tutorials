package io.confluent.strata;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class RecordPairFactory {
    Schema _schema = null;
    RecordPairFactory(String name, String namespace, Schema left, Schema right) {
        _schema = SchemaBuilder
                .record("name")
                .namespace(namespace)
                .fields()
                .name("left")
                .type(left)
                .noDefault()
                .name("right")
                .type(right)
                .noDefault()
                .endRecord();
    }

    GenericRecord newPair() {
        return newPair(null);
    }

    GenericRecord newPair(GenericData reuse) {
        return (GenericData.Record) GenericData.get().newRecord(reuse, _schema);
    }

    GenericRecord of(GenericRecord left, GenericRecord right) {
        GenericRecord newRecord = newPair();
        newRecord.put("left", left);
        newRecord.put("right", right);
        return newRecord;
    }

}
