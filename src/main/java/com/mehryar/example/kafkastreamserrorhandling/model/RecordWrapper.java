package com.mehryar.example.kafkastreamserrorhandling.model;


import com.example.mehryar.kafkastreamserrorhandling.model.ErrorMetadata;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

@SuppressWarnings("unchecked")
public class RecordWrapper<T> {
    private String status;
    // TODO: For exceptions being passed down, we will need an optional parameter here as well.
    private ErrorMetadata errorMetadata;
    private T data;

    public RecordWrapper() {
        this.status = "SUCCESS";
        this.data = null;
    }

    public RecordWrapper(String status, T data) {
        this.status = status;
        this.data = data;
    }

    public RecordWrapper(String status, ErrorMetadata errorMetadata,  T data) {
        this.status = status;
        this.data = data;
        this.errorMetadata = errorMetadata;
    }

    public ErrorMetadata getErrorMetadata() {
        return errorMetadata;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public GenericRecord getGenericRecord() {
        Schema schema = this.getSchema();
        return new GenericRecordBuilder(schema)
                .set("status", this.getStatus())
                .set("data", this.getData()).build();
    }

    public Schema getSchema() {
        return SchemaBuilder.builder()
                .record("RecordWrapper")
                .namespace(this.getClass().getPackage().getName())
                .fields()
                .name("status")
                .type(ReflectData.get().getSchema(RecordStatus.class)).noDefault()
                .name("data")
                .type(ReflectData.get().getSchema(this.getData().getClass())).noDefault().endRecord();
    }

    public T getDataSpecificRecord(GenericRecord genericRecord) {
        Schema dataSchema = ReflectData.get().getSchema(this.getData().getClass());
        return (T) SpecificData.get().deepCopy(dataSchema, genericRecord.get("data"));
    }

    public RecordWrapper<T> getRecordWrapperFromGenericRecord(GenericRecord genericRecord) {
        Schema statusSchema = ReflectData.get().getSchema(this.getStatus().getClass());
        String status = (String) genericRecord.get("status");
        return new RecordWrapper<>(status, getDataSpecificRecord(genericRecord));
    }
}
