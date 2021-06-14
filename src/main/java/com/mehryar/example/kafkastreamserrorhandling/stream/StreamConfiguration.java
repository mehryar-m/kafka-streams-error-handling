package com.mehryar.example.kafkastreamserrorhandling.stream;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Configuration
@Data
public class StreamConfiguration {
    @Value("${stream.string.inputTopic}")
    private String inputTopic;

    @Value("${stream.string.outputTopic}")
    private String outputTopic;

    @Value("${stream.avro.inputA}")
    private String avroInputA;

    @Value("${stream.avro.inputB}")
    private String avroInputB;

    @Value("${stream.avro.output}")
    private String avroOutput;

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryURL;

}
