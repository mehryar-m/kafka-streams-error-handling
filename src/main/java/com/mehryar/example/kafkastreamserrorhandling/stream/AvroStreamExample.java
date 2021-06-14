package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.example.mehryar.kafkastreamserrorhandling.model.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import com.mehryar.example.kafkastreamserrorhandling.mapper.MockErrorMapperExample;
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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;

@Component
@SuppressWarnings("unchecked")
public class AvroStreamExample {

    private final StreamConfiguration streamConfiguration;
    private final ErrorHandler errorHandler;

    @Autowired
    public AvroStreamExample(StreamConfiguration streamConfiguration, ErrorHandler errorHandler) {
        this.streamConfiguration = streamConfiguration;
        this.errorHandler = errorHandler;
    }

    public void buildExampleAvroStream(StreamsBuilder streamsBuilder) {

        KStream<String, RecordWrapper<NestedMockSchema>> nestedMockSchemaKStream =
                errorHandler.startTopology(streamsBuilder, streamConfiguration.getAvroInputA(), getNestedMockSchemaSerde());

        nestedMockSchemaKStream.mapValues(new MockErrorMapperExample()); // mock mapper that errors out if a bad thing happened.

        errorHandler.completeTopology(streamConfiguration.getAvroOutput(), nestedMockSchemaKStream);
    }


    public StreamsBuilder buildJoinedAvroStream(StreamsBuilder streamsBuilder) {

        KStream<String, RecordWrapper<NestedMockSchema>> nestedMockSchemaKStreamA =
                errorHandler.startTopology(streamsBuilder, streamConfiguration.getAvroInputA(), getNestedMockSchemaSerde());

        KStream<String, RecordWrapper<NestedMockSchema>> nestedMockSchemaKStreamB =
                errorHandler.startTopology(streamsBuilder, streamConfiguration.getAvroInputB(), getNestedMockSchemaSerde());


        KTable<String, GenericRecord> nestedMockSchemaTableA = nestedMockSchemaKStreamA
                .mapValues(new MockErrorMapperExample())
                .mapValues(RecordWrapper::getGenericRecord)
                .toTable(Materialized.with(Serdes.String(), getRecordWrapperSchemaSerde())); // mock mapper that errors out if a bad thing happened.

        KTable<String, GenericRecord> nestedMockSchemaTableB = nestedMockSchemaKStreamB
                .mapValues(new MockErrorMapperExample())
                .mapValues(RecordWrapper::getGenericRecord)
                .toTable(Materialized.with(Serdes.String(), getRecordWrapperSchemaSerde()));

        KStream<String, RecordWrapper<NestedMockSchema>> stream = nestedMockSchemaTableA.outerJoin(nestedMockSchemaTableB,
                (ValueJoiner<GenericRecord, GenericRecord, RecordWrapper<NestedMockSchema>>) (value1, value2) -> {
                    ErrorHandler errorHandler = new ErrorHandler();
                    if (value1 == null) {
                        return errorHandler.getRecordWrapperFromGenericRecord(value2, NestedMockSchema.class);
                    }
                    if (value2 == null) {
                        RecordWrapper badWrapped = errorHandler.getRecordWrapperFromGenericRecord(value1, NestedMockSchema.class);
                        badWrapped.setStatus(RecordStatus.BAD_JOIN);
                        return badWrapped;
                    }
                    NestedMockSchema nestedMockSchemaV1 = errorHandler.<NestedMockSchema>getRecordWrapperFromGenericRecord(value1, NestedMockSchema.class).getData();
                    NestedMockSchema nestedMockSchemaV2 = errorHandler.<NestedMockSchema>getRecordWrapperFromGenericRecord(value2, NestedMockSchema.class).getData();

                    NestedMockSchema nestedMockSchema = NestedMockSchema.newBuilder()
                            .setSomeParentString(nestedMockSchemaV1.getSomeParentString() + nestedMockSchemaV2.getSomeParentString())
                            .setMockSchema(nestedMockSchemaV1.getMockSchema()).build();
                    return new RecordWrapper<>(RecordStatus.SUCCESS, nestedMockSchema);
                }).toStream();

        errorHandler.completeTopology(streamConfiguration.getAvroOutput(), stream);

        return streamsBuilder;
    }


    private Serde<NestedMockSchema> getNestedMockSchemaSerde() {
        Serde<NestedMockSchema> nestedMockSchemaSerde = Serdes.serdeFrom(
                new SpecificAvroSerializer<>(), new SpecificAvroDeserializer<>());
        nestedMockSchemaSerde.configure(getSerdeConfig(), false);

        return nestedMockSchemaSerde;
    }

    private Serde<GenericRecord> getRecordWrapperSchemaSerde() {
        Serde<GenericRecord> recordWrapperSerde = Serdes.serdeFrom(
                new GenericAvroSerializer(), new GenericAvroDeserializer());
        recordWrapperSerde.configure(getSerdeConfig(), false);
        return recordWrapperSerde;
    }

    private Map<String, String> getSerdeConfig() {
        return Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                this.streamConfiguration.getSchemaRegistryURL());
    }

}
