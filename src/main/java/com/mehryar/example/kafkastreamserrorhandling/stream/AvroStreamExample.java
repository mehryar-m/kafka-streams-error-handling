package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.example.mehryar.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component
@SuppressWarnings("unchecked")
public class AvroStreamExample {


    final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "http://localhost:8081");
    private final StreamConfiguration streamConfiguration;
    private final ErrorHandler errorHandler;


    @Autowired
    public AvroStreamExample(StreamConfiguration streamConfiguration, ErrorHandler errorHandler){
        this.streamConfiguration = streamConfiguration;
        this.errorHandler = errorHandler;
    }

    public void buildExampleAvroStream(StreamsBuilder streamsBuilder){

        KStream<String, RecordWrapper<NestedMockSchema>> nestedMockSchemaKStream = streamsBuilder
                .stream(streamConfiguration.getAvroInput(), Consumed.with(Serdes.String(), getNestedMockSchemaSerde()))
                .mapValues(errorHandler.wrapper());
        KStream<String, RecordWrapper<NestedMockSchema>>[] branches = errorHandler.branchError(nestedMockSchemaKStream);
        errorHandler.handleError(branches[errorHandler.getFailIndex()]);
        branches[errorHandler.getSuccessIndex()].mapValues(RecordWrapper::getGenericRecord).to("AvroSuccess");
    }


    private <T> KStream<String, RecordWrapper<T>>[] branchError(KStream<String, RecordWrapper<T>> stream){
        return stream.branch(
                (key, value) -> value.getStatus().equals(RecordStatus.SUCCESS),
                (key, value) -> !value.getStatus().equals(RecordStatus.SUCCESS));
    }

    private Serde<NestedMockSchema> getNestedMockSchemaSerde(){
        Serde<NestedMockSchema> nestedMockSchemaSerde = Serdes.serdeFrom(
                new SpecificAvroSerializer<>(), new SpecificAvroDeserializer<>());
        nestedMockSchemaSerde.configure(serdeConfig, false);

        return nestedMockSchemaSerde;
    }

    private Serde<GenericRecord> getRecordWrapperSchemaSerde(){
        Serde<GenericRecord> recordWrapperSerde = Serdes.serdeFrom(
                new GenericAvroSerializer(), new GenericAvroDeserializer());
        recordWrapperSerde.configure(serdeConfig, false);

        return recordWrapperSerde;
    }
}
