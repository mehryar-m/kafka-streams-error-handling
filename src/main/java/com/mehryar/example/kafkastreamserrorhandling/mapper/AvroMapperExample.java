package com.mehryar.example.kafkastreamserrorhandling.mapper;

import com.example.mehryar.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class AvroMapperExample implements ValueMapper<NestedMockSchema, RecordWrapper<NestedMockSchema>> {


    @Override
    public RecordWrapper<NestedMockSchema> apply(NestedMockSchema value) {
        return RecordWrapper.<NestedMockSchema>builder()
                .status(RecordStatus.SUCCESS).data(value).build();
    }
}
