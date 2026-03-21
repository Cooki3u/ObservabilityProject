package org.example.kafkastreamsdemo.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Service
public class MetricsService {

    private static final int MAX_LENGTH_BUCKET = 10;
    private static final String OVERFLOW_BUCKET = "11_plus";

    private final Counter messagesConsumed;
    private final Counter messagesProduced;
    private final Timer processingTimer;
    private final Counter ilayNameCounter;
    private final Counter emptyNameCounter;
    private final Map<String, Counter> nameLengthCounters;

    public MetricsService(MeterRegistry registry, StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.messagesConsumed = registry.counter("messages_consumed_total");
        this.messagesProduced = registry.counter("messages_produced_total");
        this.processingTimer = registry.timer("message_processing_latency");
        this.ilayNameCounter = registry.counter("messages_name_total", "name", "Ilay");
        this.emptyNameCounter = registry.counter("messages_name_total", "name", "empty");
        this.nameLengthCounters = initNameLengthCounters(registry);

        registry.gauge("number_of_active_stream_threads", streamsBuilderFactoryBean, factoryBean -> {
            KafkaStreams streams = factoryBean.getKafkaStreams();
            return streams == null ? 0 : streams.metadataForLocalThreads().size();
        });
    }

    public void incrementConsumed() {
        messagesConsumed.increment();
    }

    public void incrementProduced() {
        messagesProduced.increment();
    }

    public void recordProcessing(Duration duration) {
        if (duration == null) {
            return;
        }
        processingTimer.record(duration);
    }

    public void recordNameMetrics(String name) {
        String trimmed = name == null ? "" : name.trim();
        int length = trimmed.length();
        String bucket = length > MAX_LENGTH_BUCKET ? OVERFLOW_BUCKET : String.valueOf(length);
        Counter lengthCounter = nameLengthCounters.get(bucket);
        if (lengthCounter != null) {
            lengthCounter.increment();
        }
        if (trimmed.isEmpty()) {
            emptyNameCounter.increment();
        } else if ("Ilay".equalsIgnoreCase(trimmed)) {
            ilayNameCounter.increment();
        }
    }

    private Map<String, Counter> initNameLengthCounters(MeterRegistry registry) {
        Map<String, Counter> counters = new HashMap<>();
        for (int i = 0; i <= MAX_LENGTH_BUCKET; i++) {
            counters.put(String.valueOf(i), registry.counter("messages_name_length_total", "length", String.valueOf(i)));
        }
        counters.put(OVERFLOW_BUCKET, registry.counter("messages_name_length_total", "length", OVERFLOW_BUCKET));
        return counters;
    }
}
