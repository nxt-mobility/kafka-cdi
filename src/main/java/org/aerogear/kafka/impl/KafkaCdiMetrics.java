package org.aerogear.kafka.impl;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicLong;

@ApplicationScoped
public class KafkaCdiMetrics {

    private final AtomicLong createdConsumers = new AtomicLong();
    private final AtomicLong startedConsumers = new AtomicLong();
    private final AtomicLong closedConsumers = new AtomicLong();

    public long getCreatedConsumers() {
        return createdConsumers.get();
    }

    public long getStartedConsumers() {
        return startedConsumers.get();
    }

    public long getClosedConsumers() {
        return closedConsumers.get();
    }

    public boolean healthy() {
        long created = getCreatedConsumers();
        long started = getStartedConsumers();
        long closed = getClosedConsumers();

        return closed == 0 && started >= created;
    }

    void consumerCreated() {
        createdConsumers.incrementAndGet();
    }

    void consumerStarted() {
        startedConsumers.incrementAndGet();
    }

    void consumerClosed() {
        closedConsumers.incrementAndGet();
    }
}
