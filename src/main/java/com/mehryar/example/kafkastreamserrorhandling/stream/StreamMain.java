package com.mehryar.example.kafkastreamserrorhandling.stream;

import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import com.mehryar.example.kafkastreamserrorhandling.mapper.StringMapperExample;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Collections;
import java.util.Map;


@Configuration
@EnableKafkaStreams
@SuppressWarnings("unchecked")
public class StreamMain {


    final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "http://localhost:8081");
    private final StreamConfiguration streamConfiguration;
    private final ErrorHandler errorHandler;


    @Autowired
    public StreamMain(StreamConfiguration streamConfiguration, ErrorHandler errorHandler){
        this.streamConfiguration = streamConfiguration;
        this.errorHandler = errorHandler;
    }

    @Bean
    public StreamsBuilder mainStream(StreamsBuilder streamsBuilder){
        AvroStreamExample avroStreamExample = new AvroStreamExample(streamConfiguration, errorHandler);
        avroStreamExample.buildExampleAvroStream(streamsBuilder);
        return streamsBuilder;
    }
}
