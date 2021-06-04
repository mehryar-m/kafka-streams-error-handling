package com.mehryar.example.kafkastreamserrorhandling.errorhandler;

import com.mehryar.example.kafkastreamserrorhandling.model.ErrorRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

public class ErrorTransfomerSupplier implements ValueTransformerSupplier<RecordWrapper, ErrorRecord> {
    @Override
    public ValueTransformer<RecordWrapper, ErrorRecord> get() {
        return new ErrorTransformer();
    }
}
