package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;


@Configuration
@EnableKafkaStreams
@SuppressWarnings("unchecked")
public class StreamMain {


    private final StreamConfiguration streamConfiguration;
    private final ErrorHandler errorHandler;


    @Autowired
    public StreamMain(StreamConfiguration streamConfiguration, ErrorHandler errorHandler) {
        this.streamConfiguration = streamConfiguration;
        this.errorHandler = errorHandler;
    }

    @Bean
    public StreamsBuilder mainStream(StreamsBuilder streamsBuilder) {
        AvroStreamExample avroStreamExample = new AvroStreamExample(streamConfiguration, errorHandler);
        avroStreamExample.buildExampleAvroStream(streamsBuilder);
        return streamsBuilder;
    }
}
