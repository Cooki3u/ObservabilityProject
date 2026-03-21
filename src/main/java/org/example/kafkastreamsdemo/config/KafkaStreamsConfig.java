package org.example.kafkastreamsdemo.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import org.apache.kafka.clients.admin.NewTopic;
import org.example.kafkastreamsdemo.model.InputMessage;
import org.example.kafkastreamsdemo.model.OutputMessage;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.KafkaStreamsCustomizer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.util.Map;

@Configuration
public class KafkaStreamsConfig {

    @Bean
    public JsonSerde<InputMessage> inputMessageSerde(ObjectMapper objectMapper) {
        JsonSerde<InputMessage> serde = new JsonSerde<>(InputMessage.class, objectMapper);
        serde.configure(Map.of(JsonDeserializer.TRUSTED_PACKAGES, "org.example.kafkastreamsdemo.model"), false);
        return serde;
    }

    @Bean
    public JsonSerde<OutputMessage> outputMessageSerde(ObjectMapper objectMapper) {
        JsonSerde<OutputMessage> serde = new JsonSerde<>(OutputMessage.class, objectMapper);
        serde.configure(Map.of(JsonDeserializer.TRUSTED_PACKAGES, "org.example.kafkastreamsdemo.model"), false);
        return serde;
    }

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name("input-topic").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name("output-topic").partitions(1).replicas(1).build();
    }

    @Bean
    public KafkaStreamsCustomizer kafkaStreamsCustomizer(MeterRegistry registry) {
        return kafkaStreams -> new KafkaStreamsMetrics(kafkaStreams).bindTo(registry);
    }
}
