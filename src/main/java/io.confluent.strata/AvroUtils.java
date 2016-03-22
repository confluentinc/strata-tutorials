package io.confluent.strata;

import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jadler on 3/21/16.
 */
public class AvroUtils {

    public static Schema addFieldsToSchema(Schema oldschema, List<Schema.Field> moreFields) {
        System.err.printf("addFieldsToSchema called with old schema of type %s\n", oldschema.getType());
        Schema newSchema = Schema.createRecord(
                oldschema.getName(), oldschema.getDoc(), oldschema.getNamespace(), oldschema.isError()
        );
        List<Schema.Field> newFields = new ArrayList(moreFields);
        for (Schema.Field field : oldschema.getFields())
            newFields.add(
                    new Schema.Field(
                            field.name(),
                            copySchema(field.schema()),
                            field.doc(),
                            field.defaultValue(),
                            field.order()));
        for (Schema.Field f : newFields)
            System.err.printf("\t%s %s %s %d\n", f.name(), f.order(), f.schema(), f.pos());

        newSchema.setFields(newFields);
        return newSchema;
    }

    public static Schema copySchema(Schema thisSchema) {
        switch (thisSchema.getType()) {
            case RECORD:
                List<Schema.Field> fields = Lists.newArrayList();
                for (Schema.Field f : thisSchema.getFields())
                    fields.add(new Schema.Field(f.name(), copySchema(f.schema()), f.doc(), f.defaultValue(), f.order()));
                Schema newSchema = Schema.createRecord(fields);
                newSchema.setFields(fields);
                return newSchema;
            case ENUM:
                return Schema.createEnum(
                        thisSchema.getName(), thisSchema.getDoc(), thisSchema.getNamespace(),
                        Lists.newArrayList(thisSchema.getEnumSymbols()));
            case ARRAY:
                return Schema.createArray(copySchema(thisSchema.getElementType()));
            case MAP:
                return Schema.createMap(copySchema(thisSchema.getValueType()));
            case UNION:
                return Schema.createUnion(Lists.newArrayList(thisSchema.getTypes()));
            case FIXED:
                return Schema.createFixed(
                        thisSchema.getName(),
                        thisSchema.getDoc(),
                        thisSchema.getNamespace(),
                        thisSchema.getFixedSize());
            case STRING:
                return Schema.create(Schema.Type.STRING);
            case BYTES:
                return Schema.create(Schema.Type.BYTES);
            case INT:
                return Schema.create(Schema.Type.INT);
            case LONG:
                return Schema.create(Schema.Type.LONG);
            case FLOAT:
                return Schema.create(Schema.Type.FLOAT);
            case DOUBLE:
                return Schema.create(Schema.Type.DOUBLE);
            case BOOLEAN:
                return Schema.create(Schema.Type.BOOLEAN);
            case NULL:
                return Schema.create(Schema.Type.NULL);
        }
        return null;

    }

    public static GenericData.Record copyRecord(GenericRecord from, Schema toSchema) {
        GenericData.Record genericDataRecord = new GenericData.Record(toSchema);
        for (Schema.Field field : from.getSchema().getFields()) {
            genericDataRecord.put(field.name(), from.get(field.pos()));
        }
        return genericDataRecord;
    }


}
