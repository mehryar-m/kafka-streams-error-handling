package com.mehryar.example.kafkastreamserrorhandling.errorhandler;

import com.example.mehryar.Error;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.kstream.ValueTransformerSupplier;

public class  ErrorTransfomerSupplier implements ValueTransformerSupplier<RecordWrapper, Error> {
    @Override
    public ValueTransformer<RecordWrapper, Error> get() {
        return new ErrorTransformer();
    }
}
