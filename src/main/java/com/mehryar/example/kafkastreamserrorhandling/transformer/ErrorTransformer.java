package com.mehryar.example.kafkastreamserrorhandling.transformer;

import com.mehryar.example.kafkastreamserrorhandling.model.ErrorRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class ErrorTransformer implements ValueTransformer<RecordWrapper, ErrorRecord> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

    }

    @Override
    public ErrorRecord transform(RecordWrapper value) {

        return ErrorRecord.builder()
                .applicationId(context.applicationId())
                .code(value.getStatus())
                .srcTopic(context.topic())
                .srcPartition(context.partition())
                .srcOffset(context.offset())
                .build();
    }

    @Override
    public void close() {

    }
}
