package com.mehryar.example.kafkastreamserrorhandling.errorhandler;

import com.example.mehryar.Error;
import com.example.mehryar.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.model.ErrorRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import com.mehryar.example.kafkastreamserrorhandling.stream.StreamConfiguration;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import jdk.nashorn.internal.objects.annotations.Property;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component
@SuppressWarnings("unchecked")
public class ErrorHandler {

    public static int SUCCESS_INDEX = 0;
    public static int FAIL_INDEX = 1;
    private final Map<String, String> serdeConfig;

    public ErrorHandler(StreamConfiguration streamConfiguration){
        this.serdeConfig = getSerdeConfig(streamConfiguration.getSchemaRegistryURL());
    }

    public  <T> KStream<String, RecordWrapper<T>>[] branchError(KStream<String, RecordWrapper<T>> stream){
        assert stream != null;
        return stream.branch(
                (key, value) -> value.getStatus().equals(RecordStatus.SUCCESS),
                (key, value) -> !value.getStatus().equals(RecordStatus.SUCCESS));
    }

    public <T> void publishError(KStream<String, RecordWrapper<T>> stream) {
        stream.transformValues(new ErrorTransfomerSupplier())
                .to("Error", getSerdes());
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

    public <T> void completeTopology(String topicName, KStream<String, RecordWrapper<T>> stream){
        KStream<String, RecordWrapper<T>>[] branches = this.branchError(stream);
        this.publishError(branches[this.getFailIndex()]);
        branches[this.getSuccessIndex()].mapValues(RecordWrapper::getData)
                .to(topicName);
    }

    private Produced<String,Error> getSerdes(){
        return Produced.with(Serdes.String(),getErrorSerde());
    }

    private Map<String, String> getSerdeConfig(String schemaRegistryUrl){
        return Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl);
    }

    private Serde<Error> getErrorSerde(){
        Serde<Error> errorSerde = Serdes.serdeFrom(
                new SpecificAvroSerializer<>(), new SpecificAvroDeserializer<>());
        errorSerde.configure(this.serdeConfig, false);

        return errorSerde;
    }

    public <T> KStream<String, RecordWrapper<T>> startTopology(StreamsBuilder streamsBuilder,
                                                                String inputTopic, Serde<T> serde){
        return streamsBuilder
                .stream(inputTopic, Consumed.with(Serdes.String(), serde))
                .mapValues(this.wrapper());
    }
}
