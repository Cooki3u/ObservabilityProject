package org.example.kafkastreamsdemo.topology;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.example.kafkastreamsdemo.metrics.MetricsService;
import org.example.kafkastreamsdemo.model.InputMessage;
import org.example.kafkastreamsdemo.model.OutputMessage;
import org.example.kafkastreamsdemo.service.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class StreamsTopology {

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    private static final Logger log = LoggerFactory.getLogger(StreamsTopology.class);

    @Bean
    public KStream<String, OutputMessage> streamTopology(StreamsBuilder builder,
                                                         Serde<InputMessage> inputMessageSerde,
                                                         Serde<OutputMessage> outputMessageSerde,
                                                         MetricsService metricsService,
                                                         MessageProcessor messageProcessor) {

        KStream<String, InputMessage> input = builder.stream(
                INPUT_TOPIC,
                Consumed.with(Serdes.String(), inputMessageSerde)
        );

        KStream<String, OutputMessage> output = input
                .peek((key, value) -> metricsService.incrementConsumed())
                .mapValues(value -> {
                    log.debug("Processing message: value={}", value);
                    return messageProcessor.process(value);
                })
                .peek((key, value) -> metricsService.incrementProduced());

        output.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), outputMessageSerde));

        return output;
    }
}
