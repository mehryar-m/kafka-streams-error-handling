package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.mehryar.example.kafkastreamserrorhandling.model.ErrorRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.ExampleRecord;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ErrorTransformer implements ValueTransformer<ExampleRecord, ErrorRecord> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

    }

    @Override
    public ErrorRecord transform(ExampleRecord value) {

        return ErrorRecord.builder().srcOffset(context.offset()).build();
    }

    @Override
    public void close() {

    }
}
