package com.mehryar.example.kafkastreamserrorhandling.stream;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;


@Configuration
@Data
public class StreamConfiguration {
    @Value("${stream.inputTopic}")
    private String inputTopic;

    @Value("${stream.outputTopic}")
    private String outputTopic;
}
