package org.example.kafkastreamsdemo.service;

import org.example.kafkastreamsdemo.metrics.MetricsService;
import org.example.kafkastreamsdemo.simulation.SimulationProperties;
import org.example.kafkastreamsdemo.model.InputMessage;
import org.example.kafkastreamsdemo.model.OutputMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class MessageProcessor {

    private static final Logger log = LoggerFactory.getLogger(MessageProcessor.class);

    private final MetricsService metricsService;
    private final SimulationProperties simulationProperties;

    public MessageProcessor(MetricsService metricsService, SimulationProperties simulationProperties) {
        this.metricsService = metricsService;
        this.simulationProperties = simulationProperties;
    }

    public OutputMessage process(InputMessage input) {
        long start = System.nanoTime();
        try {
            // Simulate processing latency for visualization purposes.
            int sleepMs = ThreadLocalRandom.current().nextInt(0, 4001);
            if (simulationProperties.isEnabled()) {
                int minMs = Math.max(0, simulationProperties.getMinSleepMs());
                int maxMs = Math.max(minMs, simulationProperties.getMaxSleepMs());
                sleepMs = ThreadLocalRandom.current().nextInt(minMs, maxMs + 1);
                double spikeProbability = simulationProperties.getSpikeProbability();
                if (spikeProbability > 0.0 && ThreadLocalRandom.current().nextDouble() < spikeProbability) {
                    sleepMs = Math.max(sleepMs, simulationProperties.getSpikeSleepMs());
                }
            }
            Thread.sleep(sleepMs);

            String name = input == null ? null : input.getName();
            String trimmed = name == null ? "" : name.trim();
            String greet = trimmed.isEmpty() ? "Hello" : "Hello " + trimmed;
            metricsService.recordNameMetrics(trimmed);
            return new OutputMessage(name, greet);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while simulating latency", ex);
            return new OutputMessage(null, "Hello");
        } catch (Exception ex) {
            log.warn("Failed to process message: {}", input, ex);
            return new OutputMessage(null, "Hello");
        } finally {
            metricsService.recordProcessing(Duration.ofNanos(System.nanoTime() - start));
        }
    }
}
