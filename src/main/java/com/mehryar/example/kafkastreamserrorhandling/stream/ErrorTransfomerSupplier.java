package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.mehryar.example.kafkastreamserrorhandling.model.ErrorRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.ExampleRecord;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

public class ErrorTransfomerSupplier implements ValueTransformerSupplier<ExampleRecord, ErrorRecord> {
    @Override
    public ValueTransformer<ExampleRecord, ErrorRecord> get() {
        return new ErrorTransformer();
    }
}
