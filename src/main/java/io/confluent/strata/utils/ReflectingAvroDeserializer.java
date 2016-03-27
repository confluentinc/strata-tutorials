package io.confluent.strata.utils;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.util.Map;

/**
 * Created by jadler on 3/22/16.
 */
public class ReflectingAvroDeserializer<O> implements Deserializer<O> {

    public ReflectingAvroDeserializer() {
        super();
        ParameterizedType superClass = (ParameterizedType) getClass().getGenericSuperclass();
        Class<O> clazz = (Class<O>) superClass.getActualTypeArguments()[0];
        schema = ReflectData.get().getSchema(clazz);
        reflectDatumReader = new ReflectDatumReader<O>(schema);
    }

    Schema schema = null;
    private DatumReader<O> reflectDatumReader = null;

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public O deserialize(String s, byte[] bytes) {
        try {
            SeekableInput input = new SeekableByteArrayInput(bytes);
            FileReader<O> reader = DataFileReader.openReader(input, reflectDatumReader);
            return reader.next();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void close() {

    }
}
