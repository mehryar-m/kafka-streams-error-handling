package com.mehryar.example.kafkastreamserrorhandling.stream;


import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class StreamTests {

    private static final StreamConfiguration streamConfiguration = new StreamConfiguration();
    TopologyTestDriver topologyTestDriver;
    private static Properties props;

    @BeforeAll
    public static void setup() {
        streamConfiguration.setInputTopic("A");
        streamConfiguration.setOutputTopic("B");
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-stream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    }
    @Test
    void testTopologyForError() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        ErrorHandler errorHandler = new ErrorHandler();
        new StringStreamExample(streamConfiguration, errorHandler).stringStream(streamsBuilder);
        Topology topology = streamsBuilder.build();
        topologyTestDriver = new TopologyTestDriver(topology, props);

        TestInputTopic<String, String> inputTopic = topologyTestDriver.createInputTopic("A", new Serdes.StringSerde().serializer(), new Serdes.StringSerde().serializer());
        TestOutputTopic<String, String> outputTopic = topologyTestDriver.createOutputTopic("Success", new Serdes.StringSerde().deserializer(), new Serdes.StringSerde().deserializer());
        TestOutputTopic<String, String> errorTopic = topologyTestDriver.createOutputTopic("Error", new Serdes.StringSerde().deserializer(), new Serdes.StringSerde().deserializer());
        inputTopic.pipeInput("good");
        assert !outputTopic.isEmpty();
        assert errorTopic.isEmpty();

        inputTopic.pipeInput("not sgood");
        assert !errorTopic.isEmpty();

    }

}
