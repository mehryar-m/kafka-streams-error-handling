package com.mehryar.example.kafkastreamserrorhandling.mapper;

import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class StringMapperExample implements ValueMapper<String, RecordWrapper<String>> {


    @Override
    public RecordWrapper<String> apply(String value) {
        return !value.equals("good") ? wrapError(value) : wrapSuccess(value);
    }

    private RecordWrapper<String> wrapError(String value) {
        return RecordWrapper.<String>builder().status(RecordStatus.BAD_MAPPING).data(value).build();
    }

    private RecordWrapper wrapSuccess(String value) {
        return RecordWrapper.builder()
                .status(RecordStatus.SUCCESS)
                .data(value)
                .build();
    }
}
