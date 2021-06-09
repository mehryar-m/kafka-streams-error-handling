package com.mehryar.example.kafkastreamserrorhandling.errorhandler;

import com.mehryar.example.kafkastreamserrorhandling.model.ErrorRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.stereotype.Component;

@Component
@SuppressWarnings("unchecked")
public class ErrorHandler {

    public static int SUCCESS_INDEX = 0;
    public static int FAIL_INDEX = 1;


    public  <T> KStream<String, RecordWrapper<T>>[] branchError(KStream<String, RecordWrapper<T>> stream){
        assert stream != null;
        return stream.branch(
                (key, value) -> value.getStatus().equals(RecordStatus.SUCCESS),
                (key, value) -> !value.getStatus().equals(RecordStatus.SUCCESS));
    }

    public <T> void publishError(KStream<String, RecordWrapper<T>> stream) {
        stream.transformValues(new ErrorTransfomerSupplier()).mapValues(ErrorRecord::toString).to("Error");
    }

    public int getFailIndex() {
        return FAIL_INDEX;
    }

    public  int getSuccessIndex() {
        return SUCCESS_INDEX;
    }

    public <T> ValueMapper<T, RecordWrapper<T>> wrapper(){
        return value -> RecordWrapper.<T>builder()
                .status(RecordStatus.SUCCESS).data(value).build();
    }
}
