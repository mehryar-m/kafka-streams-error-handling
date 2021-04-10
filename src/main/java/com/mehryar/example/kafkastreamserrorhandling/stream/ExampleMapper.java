package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.mehryar.example.kafkastreamserrorhandling.model.ExampleRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.ExampleRecordState;
import org.apache.kafka.streams.kstream.ValueMapper;

public class ExampleMapper implements ValueMapper<String, ExampleRecord> {

    @Override
    public ExampleRecord apply(String value) {
        return !value.equals("good") ? wrapError(value) : wrapSuccess(value);
    }

    private ExampleRecord wrapError(String value) {
        return ExampleRecord.builder().data(value).state(ExampleRecordState.FAIL).build();
    }

    private ExampleRecord wrapSuccess(String value) {
        return ExampleRecord.builder().data(value).state(ExampleRecordState.SUCCESS).build();
    }
}
