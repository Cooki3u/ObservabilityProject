package org.example.kafkastreamsdemo.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class KafkaConsumerLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerLagMetrics.class);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final AdminClient adminClient;
    private final MeterRegistry registry;
    private final String groupId;
    private final String topic;
    private final boolean enabled;
    private final Map<TopicPartition, AtomicLong> lagByPartition = new ConcurrentHashMap<>();
    private final AtomicLong totalLag = new AtomicLong(0);

    public KafkaConsumerLagMetrics(KafkaProperties kafkaProperties,
                                   MeterRegistry registry,
                                   @Value("${app.lag.topic:input-topic}") String topic,
                                   @Value("${app.lag.enabled:true}") boolean enabled) {
        this.registry = registry;
        this.topic = topic;
        this.enabled = enabled;
        this.groupId = kafkaProperties.getStreams() != null ? kafkaProperties.getStreams().getApplicationId() : null;
        if (!enabled || groupId == null || groupId.isBlank()) {
            this.adminClient = null;
            return;
        }
        this.adminClient = AdminClient.create(kafkaProperties.buildAdminProperties());
        Gauge.builder("kafka_consumer_group_lag_total", totalLag, AtomicLong::get)
                .description("Total consumer group lag across partitions")
                .tag("topic", this.topic)
                .tag("group", this.groupId)
                .register(this.registry);
    }

    @Scheduled(fixedDelayString = "${app.lag.refresh-ms:5000}")
    public void refreshLag() {
        if (!enabled || adminClient == null) {
            return;
        }
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = adminClient
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            if (committed == null || committed.isEmpty()) {
                totalLag.set(0);
                return;
            }

            Map<TopicPartition, OffsetSpec> endOffsetRequest = new HashMap<>();
            for (TopicPartition tp : committed.keySet()) {
                if (topic.equals(tp.topic())) {
                    endOffsetRequest.put(tp, OffsetSpec.latest());
                }
            }

            if (endOffsetRequest.isEmpty()) {
                totalLag.set(0);
                return;
            }

            ListOffsetsResult endOffsets = adminClient.listOffsets(endOffsetRequest);
            Map<TopicPartition, ListOffsetsResultInfo> endOffsetResults = endOffsets.all()
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            long total = 0;
            for (Map.Entry<TopicPartition, OffsetSpec> entry : endOffsetRequest.entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata committedOffset = committed.get(tp);
                ListOffsetsResultInfo endInfo = endOffsetResults.get(tp);
                if (committedOffset == null || endInfo == null) {
                    continue;
                }
                long lag = Math.max(0, endInfo.offset() - committedOffset.offset());
                total += lag;
                lagByPartition.computeIfAbsent(tp, this::registerPartitionGauge).set(lag);
            }
            totalLag.set(total);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException | TimeoutException ex) {
            log.debug("Failed to compute consumer lag", ex);
        } catch (Exception ex) {
            log.warn("Unexpected error while computing consumer lag", ex);
        }
    }

    @PreDestroy
    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    private AtomicLong registerPartitionGauge(TopicPartition tp) {
        AtomicLong value = new AtomicLong(0);
        Gauge.builder("kafka_consumer_group_lag", value, AtomicLong::get)
                .description("Consumer group lag by topic partition")
                .tag("topic", tp.topic())
                .tag("partition", String.valueOf(tp.partition()))
                .tag("group", groupId)
                .register(registry);
        return value;
    }
}
