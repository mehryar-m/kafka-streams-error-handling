package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.mehryar.example.kafkastreamserrorhandling.model.RecordCode;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueMapper;

public class ExampleMapper implements ValueMapper<String, RecordWrapper> {

    @Override
    public RecordWrapper apply(String value) {
        return !value.equals("good") ? wrapError(value) : wrapSuccess(value);
    }

    private RecordWrapper wrapError(String value) {
        return RecordWrapper.builder().state(RecordCode.BAD_MAPPING).data(value).build();
    }

    private RecordWrapper wrapSuccess(String value) {
        return RecordWrapper.builder()
                .state(RecordCode.SUCCESS)
                .data(value)
                .build();
    }
}
