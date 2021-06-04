package com.mehryar.example.kafkastreamserrorhandling.model;


import lombok.Builder;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;

@Data
@Builder
public class RecordWrapper<T> {
    private RecordStatus status;
    private T data;


    public GenericRecord getGenericRecord(){
        Schema schema = SchemaBuilder.builder()
                .record("Wrapped" + this.getData().getClass().getSimpleName())
                .namespace(this.getClass().getPackage().getName())
                .fields()
                .name("status")
                .type(ReflectData.get().getSchema(this.getStatus().getClass())).noDefault()
                .name("data")
                .type(ReflectData.get().getSchema(this.getData().getClass())).noDefault().endRecord();

        return new GenericRecordBuilder(schema)
                .set("status", this.getStatus())
                .set("data", this.getData()).build();
    }

}
