package com.mehryar.example.kafkastreamserrorhandling.stream;


import com.example.mehryar.Error;
import com.example.mehryar.MockSchema;
import com.example.mehryar.NestedMockSchema;
import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordStatus;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class StreamTests {

    private static final String SCHEMA_REGISTRY_SCOPE = StreamTests.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, NestedMockSchema> nestedMockSchemaTestInputTopic;
    private TestOutputTopic<String, Error> errorTestOutputTopic;
    private TestOutputTopic<String, NestedMockSchema> nestedMockSchemaTestOutputTopic;



    private static final StreamConfiguration streamConfiguration = new StreamConfiguration();
    private static Properties props;
    private final SpecificAvroSerde<NestedMockSchema> mockSchemaSerde = new SpecificAvroSerde<>();
    private final SpecificAvroSerde<Error> errorSerde = new SpecificAvroSerde<>();

    @BeforeEach
    public void serdePrep(){
        streamConfiguration.setAvroInput("avroInput");
        streamConfiguration.setAvroOutput("avroOutput");
        streamConfiguration.setSchemaRegistryURL("mock://" + SCHEMA_REGISTRY_SCOPE);
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        setupSerdes();
    }

    @Test
    void testTopologyForError() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        ErrorHandler errorHandler = new ErrorHandler(streamConfiguration);

        new StreamMain(streamConfiguration, errorHandler).mainStream(streamsBuilder);
        Topology topology = streamsBuilder.build();
        topologyTestDriver = new TopologyTestDriver(topology, props);

        nestedMockSchemaTestInputTopic= topologyTestDriver.createInputTopic(streamConfiguration.getAvroInput(), new Serdes.StringSerde().serializer(), mockSchemaSerde.serializer());
        nestedMockSchemaTestOutputTopic = topologyTestDriver.createOutputTopic(streamConfiguration.getAvroOutput(), new Serdes.StringSerde().deserializer(), mockSchemaSerde.deserializer());
        errorTestOutputTopic = topologyTestDriver.createOutputTopic("Error", new Serdes.StringSerde().deserializer(), errorSerde.deserializer());

        NestedMockSchema nestedMockSchema = NestedMockSchema.newBuilder()
                .setSomeParentString("parent")
                .setMockSchema(MockSchema.newBuilder().setSomeChildString("child").build()).build();

        nestedMockSchemaTestInputTopic.pipeInput("ok", nestedMockSchema);
        assert !nestedMockSchemaTestOutputTopic.isEmpty();
        nestedMockSchema.setSomeParentString("bad");
        nestedMockSchemaTestInputTopic.pipeInput("ok", nestedMockSchema);
        assert errorTestOutputTopic.readValue().getErrorCode().equals(RecordStatus.BAD_MAPPING.toString());

    }

    private void setupSerdes(){
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        mockSchemaSerde.configure(serdeConfig, false);
        errorSerde.configure(serdeConfig, false);
    }

    @AfterEach
    void afterEach() {
        topologyTestDriver.close();
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

}
