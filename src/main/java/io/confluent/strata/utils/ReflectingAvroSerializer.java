package io.confluent.strata.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Map;

/**
 * Created by jadler on 3/22/16.
 */
public class ReflectingAvroSerializer<O> implements Serializer<O> {

    public ReflectingAvroSerializer() {
        super();
        ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        Class<O> type = (Class<O>) superClass.getActualTypeArguments()[0];
        schema = ReflectData.get().getSchema(type);
        reflectDatumWriter = new ReflectDatumWriter<>(schema);
    }

    Schema schema = null;
    private ReflectDatumWriter<O> reflectDatumWriter = null;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public byte[] serialize(String topic, O data){
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            DataFileWriter<O> writer = new DataFileWriter<>(reflectDatumWriter).create(schema, outputStream);
            writer.append(data);
            writer.close();
            return outputStream.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void close() {

    }
}
