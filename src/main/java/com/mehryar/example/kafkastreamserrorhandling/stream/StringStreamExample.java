package com.mehryar.example.kafkastreamserrorhandling.stream;



import com.mehryar.example.kafkastreamserrorhandling.errorhandler.ErrorHandler;
import com.mehryar.example.kafkastreamserrorhandling.mapper.StringMapperExample;
import com.mehryar.example.kafkastreamserrorhandling.model.RecordWrapper;
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
@SuppressWarnings("unchecked")
public class StringStreamExample {

    private final StreamConfiguration streamConfiguration;
    private final ErrorHandler errorHandler;

    @Autowired
    public StringStreamExample(StreamConfiguration streamConfiguration, ErrorHandler errorHandler){
        this.streamConfiguration = streamConfiguration;
        this.errorHandler = errorHandler;
    }

    @Bean
    public StreamsBuilder stringStream(StreamsBuilder streamsBuilder){

        KStream<String, String> inputStream = streamsBuilder.stream(streamConfiguration.getInputTopic(),
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, RecordWrapper<String>>[] branches = errorHandler.branchError(inputStream.mapValues(new StringMapperExample()));
        branches[errorHandler.getSuccessIndex()].mapValues(RecordWrapper::toString).to("Success");
        errorHandler.handleError(branches[errorHandler.getFailIndex()]);
        return streamsBuilder;
    }

}
