package com.mehryar.example.kafkastreamserrorhandling.mapper;

import com.example.mehryar.kafkastreamserrorhandling.model.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class MockErrorMapperExample implements ValueMapper<RecordWrapper<NestedMockSchema>, RecordWrapper<NestedMockSchema>> {
    @Override
    public RecordWrapper<NestedMockSchema> apply(RecordWrapper<NestedMockSchema> value) {
        NestedMockSchema nestedMockSchema = value.getData();
        if (nestedMockSchema.getSomeParentString().equals("bad")) {
            value.setStatus(RecordStatus.BAD_MAPPING);
        }
        return value;
    }
}
