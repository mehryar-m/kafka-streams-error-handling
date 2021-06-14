package com.mehryar.example.kafkastreamserrorhandling;


import com.example.mehryar.kafkastreamserrorhandling.model.MockSchema;
import com.example.mehryar.kafkastreamserrorhandling.model.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class Wrappingtests {

    @Test
    public void wrappingAndUnwrappingAnAvro(){
        MockSchema mockSchema = MockSchema.newBuilder().setSomeChildString("child").build();
        NestedMockSchema nestedMockSchema = NestedMockSchema.newBuilder()
                .setMockSchema(mockSchema)
                .setSomeParentString("parent").build();

        assert wrap(nestedMockSchema).getStatus().equals(RecordStatus.SUCCESS);
        assert wrap(nestedMockSchema).getGenericRecord().get("status").equals(RecordStatus.SUCCESS);
        System.out.println();

    }

    @Test
    public void getSpecificRecord() throws IOException {
        MockSchema mockSchema = MockSchema.newBuilder().setSomeChildString("child").build();
        NestedMockSchema nestedMockSchema = NestedMockSchema.newBuilder()
                .setMockSchema(mockSchema)
                .setSomeParentString("parent").build();

        RecordWrapper recordWrapper = wrap(nestedMockSchema);
        assert recordWrapper.getData().equals(recordWrapper.getDataSpecificRecord(recordWrapper.getGenericRecord()));
    }

    @Test
    public void getRecordWrapperFromGenericRecord(){
        RecordWrapper<NestedMockSchema> recordWrapper = wrap(getFakeNestedMockSchema());
        GenericRecord genericRecord = recordWrapper.getGenericRecord();
        RecordWrapper<NestedMockSchema> newRecordWrapper = recordWrapper.getRecordWrapperFromGenericRecord(genericRecord);
        assert newRecordWrapper.getStatus().equals(recordWrapper.getStatus());
        NestedMockSchema nestedMockSchema = newRecordWrapper.getData();
        assert nestedMockSchema.getMockSchema().getSomeChildString().equals(recordWrapper.getData().getMockSchema().getSomeChildString());

        ErrorHandler errorHandler = new ErrorHandler();
        RecordWrapper<NestedMockSchema> recordWrapper1 = errorHandler.getRecordWrapperFromGenericRecord(genericRecord, NestedMockSchema.class);
        assert recordWrapper1.getStatus().equals(recordWrapper.getStatus());

    }

    private RecordWrapper<NestedMockSchema> wrap(NestedMockSchema nestedMockSchema){
        return new RecordWrapper<>(RecordStatus.SUCCESS, nestedMockSchema);
    }

    private NestedMockSchema getFakeNestedMockSchema(){
        MockSchema mockSchema = MockSchema.newBuilder().setSomeChildString("child").build();
        return NestedMockSchema.newBuilder()
                .setMockSchema(mockSchema)
                .setSomeParentString("parent").build();
    }
}
