package com.mehryar.example.kafkastreamserrorhandling.mapper;

import com.example.mehryar.kafkastreamserrorhandling.model.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class AvroMapperExample implements ValueMapper<NestedMockSchema, RecordWrapper<NestedMockSchema>> {


    @Override
    public RecordWrapper<NestedMockSchema> apply(NestedMockSchema value) {
        return new RecordWrapper<>(RecordStatus.SUCCESS, value);
    }
}
