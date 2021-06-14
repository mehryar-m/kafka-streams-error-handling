package com.mehryar.example.kafkastreamserrorhandling.errorhandler;

import com.example.mehryar.kafkastreamserrorhandling.model.Error;
import com.example.mehryar.kafkastreamserrorhandling.model.TopicMetadata;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.time.Instant;

public class ErrorTransformer implements ValueTransformer<RecordWrapper, Error> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;

    }

    @Override
    public Error transform(RecordWrapper value) {

        return Error.newBuilder()
                .setApplicationId(context.applicationId())
                .setErrorCode(value.getStatus().toString())
                .setSrcTopic(getTopicMetadata())
                .setRetryCount(1)
                .setMetaData(value.getErrorMetadata()) // TODO: exception
                .setSrcEventTimestamp(getTimestamp(context.timestamp()))
                .build();
    }

    private TopicMetadata getTopicMetadata() {
        return TopicMetadata.newBuilder()
                .setName(context.topic())
                .setOffset(context.offset())
                .setPartition(context.partition()).build();
    }

    private Instant getTimestamp(long value) {
        return Instant.ofEpochMilli(value);
    }


    @Override
    public void close() {

    }
}
