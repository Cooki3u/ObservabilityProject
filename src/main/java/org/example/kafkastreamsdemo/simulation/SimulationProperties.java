package org.example.kafkastreamsdemo.simulation;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "app.simulation")
public class SimulationProperties {

    private boolean enabled = false;
    private int minSleepMs = 0;
    private int maxSleepMs = 4000;
    private double spikeProbability = 0.0;
    private int spikeSleepMs = 0;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getMinSleepMs() {
        return minSleepMs;
    }

    public void setMinSleepMs(int minSleepMs) {
        this.minSleepMs = minSleepMs;
    }

    public int getMaxSleepMs() {
        return maxSleepMs;
    }

    public void setMaxSleepMs(int maxSleepMs) {
        this.maxSleepMs = maxSleepMs;
    }

    public double getSpikeProbability() {
        return spikeProbability;
    }

    public void setSpikeProbability(double spikeProbability) {
        this.spikeProbability = spikeProbability;
    }

    public int getSpikeSleepMs() {
        return spikeSleepMs;
    }

    public void setSpikeSleepMs(int spikeSleepMs) {
        this.spikeSleepMs = spikeSleepMs;
    }
}
