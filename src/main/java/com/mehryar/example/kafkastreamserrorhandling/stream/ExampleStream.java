package com.mehryar.example.kafkastreamserrorhandling.stream;


import com.mehryar.example.kafkastreamserrorhandling.model.ExampleRecord;
import com.mehryar.example.kafkastreamserrorhandling.model.ExampleRecordState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Configuration
@EnableKafkaStreams
public class ExampleStream {

    @Autowired
    StreamConfiguration streamConfiguration;

    @Bean
    public StreamsBuilder exampleStream(StreamsBuilder streamsBuilder){

        int SUCCESS = 0;
        int FAIL = 1;

        KStream<String, String> inputStream = streamsBuilder.stream(streamConfiguration.getInputTopic(),
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, ExampleRecord>[] branches = inputStream.mapValues(new ExampleMapper()).branch(
                (key, value) -> value.getState().equals(ExampleRecordState.SUCCESS),
                (key, value) -> value.getState().equals(ExampleRecordState.FAIL));

        branches[SUCCESS].to("Success");
        branches[FAIL].transformValues(new ErrorTransfomerSupplier()).to("Error");

        return streamsBuilder;
    }
}
