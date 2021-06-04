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

    @Value("${stream.avro.input}")
    private String avroInput;

    @Value("${stream.string.output}")
    private String avroOutput;

}
