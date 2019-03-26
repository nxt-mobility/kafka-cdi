package org.aerogear.kafka.impl;

import org.eclipse.microprofile.health.Health;
import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;

import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.atomic.AtomicLong;

@Health
@ApplicationScoped
public class KafkaCdiMetrics implements HealthCheck {

    private final AtomicLong createdConsumers = new AtomicLong();
    private final AtomicLong startedConsumers = new AtomicLong();
    private final AtomicLong closedConsumers = new AtomicLong();

    @Override
    public HealthCheckResponse call() {
        return HealthCheckResponse.named("kafka-cdi").state(healthy())
                .withData("created-consumers", getCreatedConsumers())
                .withData("started-consumers", getStartedConsumers())
                .withData("closed-consumers", getClosedConsumers())
                .build();
    }

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
