package com.mehryar.example.kafkastreamserrorhandling;

import com.example.mehryar.MockSchema;
import com.example.mehryar.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.junit.jupiter.api.Test;

public class Wrappingtests {

    @Test
    public void wrappingAndUnwrappingAnAvro(){
        MockSchema mockSchema = MockSchema.newBuilder().setSomeChildString("child").build();
        NestedMockSchema nestedMockSchema = NestedMockSchema.newBuilder()
                .setMockSchema(mockSchema)
                .setSomeParentString("parent").build();

        assert wrap(nestedMockSchema).getStatus() == RecordStatus.SUCCESS;
        assert wrap(nestedMockSchema).getGenericRecord().get("status") == RecordStatus.SUCCESS;
        System.out.println();

    }

    private RecordWrapper wrap(NestedMockSchema nestedMockSchema){
        return RecordWrapper.builder()
                .status(RecordStatus.SUCCESS)
                .data(nestedMockSchema).build();
    }
}
