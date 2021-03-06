/*
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.impl;

import org.aerogear.kafka.DefaultConsumerRebalanceListener;
import org.aerogear.kafka.cdi.annotation.Consumer;
import org.aerogear.kafka.cdi.annotation.KafkaConfig;
import org.aerogear.kafka.cdi.extension.VerySimpleEnvironmentResolver;
import org.aerogear.kafka.serialization.CafdiSerdes;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.jboss.weld.context.bound.BoundRequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.Any;
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import javax.enterprise.util.AnnotationLiteral;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class DelegationKafkaConsumer implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(DelegationKafkaConsumer.class);

    /*
     * True if a consumer is running; otherwise false
     */
    private final AtomicBoolean running = new AtomicBoolean(Boolean.TRUE);
    private final Properties properties = new Properties();

    private Object consumerInstance;
    private KafkaConsumer<?, ?> consumer;
    private List<String> topics;
    private AnnotatedMethod annotatedListenerMethod;
    private ConsumerRebalanceListener consumerRebalanceListener;

    private int numberOfRetries;
    private long retryBackoffMs;

    private Class<?>[] parameterTypes;
    private Type[] genericParameterTypes;
    private ConsumerMode mode;

    private KafkaCdiMetrics metrics;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public DelegationKafkaConsumer() {
    }


    private ConsumerRebalanceListener createConsumerRebalanceListener(final Class<? extends ConsumerRebalanceListener> consumerRebalanceListenerClazz) {

        if (consumerRebalanceListenerClazz.equals(DefaultConsumerRebalanceListener.class)) {
            return new DefaultConsumerRebalanceListener(consumer);
        } else {
            try {
                return consumerRebalanceListenerClazz.getDeclaredConstructor(org.apache.kafka.clients.consumer.Consumer.class).newInstance(consumer);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                logger.error("Could not create desired listener, using DefaultConsumerRebalanceListener", e);
                return new DefaultConsumerRebalanceListener(consumer);
            }
        }
    }

    private Class<?> consumerKeyType(final Class<?> defaultKeyType) {
        if (parameterTypes.length >= 2) {
            return parameterTypes[0];
        } else {
            if (parameterTypes.length == 1 && ConsumerRecords.class.isAssignableFrom(parameterTypes[0])) {
                return (Class<?>) ((ParameterizedType) genericParameterTypes[0]).getActualTypeArguments()[0];
            }
            return defaultKeyType;
        }
    }

    private Class<?> consumerValueType() {
        if (parameterTypes.length >= 2) {
            return parameterTypes[1];
        } else {
            if (parameterTypes.length == 1 && ConsumerRecords.class.isAssignableFrom(parameterTypes[0])) {
                return (Class<?>) ((ParameterizedType) genericParameterTypes[0]).getActualTypeArguments()[1];
            }
            return parameterTypes[0];
        }
    }

    private <K, V> void createKafkaConsumer(final Class<K> keyType, final Class<V> valueType, final Properties consumerProperties) {
        consumer = new KafkaConsumer<K, V>(consumerProperties, CafdiSerdes.serdeFrom(keyType).deserializer(), CafdiSerdes.serdeFrom(valueType).deserializer());
    }


    public void initialize(final String bootstrapServers, final AnnotatedMethod annotatedMethod, final BeanManager beanManager, final KafkaConfig kafkaConfig) {
        final Consumer consumerAnnotation = annotatedMethod.getAnnotation(Consumer.class);

        this.topics = Arrays.stream(consumerAnnotation.topics())
                .map(VerySimpleEnvironmentResolver::resolveVariables)
                .collect(Collectors.toList());

        numberOfRetries = IntStream.of(consumerAnnotation.retries(), kafkaConfig.defaultConsumerRetries()).filter(v -> v > 0).findFirst().orElse(0);
        retryBackoffMs = IntStream.of(consumerAnnotation.retryBackoffMs(), kafkaConfig.defaultConsumerRetryBackoffMs()).filter(v -> v > 0).findFirst().orElse(0);

        final String groupId = VerySimpleEnvironmentResolver.resolveVariables(consumerAnnotation.groupId());
        final Class<?> recordKeyType = consumerAnnotation.keyType();

        this.annotatedListenerMethod = annotatedMethod;
        parameterTypes = annotatedListenerMethod.getJavaMember().getParameterTypes();
        genericParameterTypes = annotatedListenerMethod.getJavaMember().getGenericParameterTypes();
        mode = getConsumerMode(parameterTypes);

        final Class<?> keyTypeClass = consumerKeyType(recordKeyType);
        final Class<?> valTypeClass = consumerValueType();

        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(GROUP_ID_CONFIG, groupId);

        IntStream.of(consumerAnnotation.fetchMaxWaitMs(), kafkaConfig.defaultFetchMaxWaitMs()).filter(value -> value > 0).findFirst().ifPresent(value -> properties.put(FETCH_MAX_WAIT_MS_CONFIG, value));
        IntStream.of(consumerAnnotation.requestTimeoutMs(), kafkaConfig.defaultRequestTimeoutMs()).filter(value -> value > 0).findFirst().ifPresent(value -> properties.put(REQUEST_TIMEOUT_MS_CONFIG, value));

        properties.put(AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset());
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, CafdiSerdes.serdeFrom(keyTypeClass).deserializer().getClass());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, CafdiSerdes.serdeFrom(valTypeClass).deserializer().getClass());

        createKafkaConsumer(keyTypeClass, valTypeClass, properties);
        this.consumerRebalanceListener = createConsumerRebalanceListener(consumerAnnotation.consumerRebalanceListener());

        final Set<Bean<?>> beans = beanManager.getBeans(annotatedListenerMethod.getJavaMember().getDeclaringClass());
        final Bean<?> propertyResolverBean = beanManager.resolve(beans);
        final CreationalContext<?> creationalContext = beanManager.createCreationalContext(propertyResolverBean);
        final Type consumerType = annotatedListenerMethod.getJavaMember().getDeclaringClass();

        consumerInstance = beanManager.getReference(propertyResolverBean, consumerType, creationalContext);

        Bean<?> metricsBean = beanManager.resolve(beanManager.getBeans(KafkaCdiMetrics.class, new AnnotationLiteral<Any>() {}));
        metrics = (KafkaCdiMetrics) beanManager.getReference(metricsBean, KafkaCdiMetrics.class, beanManager.createCreationalContext(metricsBean));
        metrics.consumerCreated();
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics, consumerRebalanceListener);
            logger.info("subscribed to {}", topics);
            pollingLoop:
            while (isRunning()) {
                long pollStart = System.currentTimeMillis();

                final ConsumerRecords<?, ?> records = consumer.poll(Duration.ofMillis(100));

                logSlowPoll(pollStart, System.currentTimeMillis());

                if (!started.getAndSet(true)) {
                    metrics.consumerStarted(); // count consumer as started after first successful poll
                }

                for (final ConsumerRecord<?, ?> record : records) {


                    BeanManager beanManager = CDI.current().getBeanManager();

                    Bean<?> bean = beanManager.resolve(beanManager.getBeans(BoundRequestContext.class));
                    CreationalContext<Object> creationalContext = beanManager.createCreationalContext(null);
                    int retries = 0;
                    boolean success = false;
                    try {
                        do {
                            try {
                                // Activate CDI request scope for each invocation (works for WELD only)
                                BoundRequestContext boundRequestContext = (BoundRequestContext) beanManager.getReference(bean, BoundRequestContext.class, creationalContext);
                                Map<String, Object> requestDataStore = new ConcurrentHashMap<>();
                                CdiRequestScopeUtils.start(boundRequestContext, requestDataStore);

                                try {
                                    if (mode == ConsumerMode.SINGLE) {
                                        dispatchSinglePayload(record);
                                    } else {
                                        dispatchCompletePayload(records);
                                        continue pollingLoop;
                                    }
                                    success = true;
                                } finally {
                                    CdiRequestScopeUtils.end(boundRequestContext, requestDataStore);
                                }
                                logger.trace("dispatched payload {} to consumer", record.value());
                            } catch (IllegalAccessException e) {
                                logger.error("error dispatching received value to consumer", e);
                                break;
                            } catch (SerializationException e) {
                                logger.error("failed to deserialize message, giving up", e);
                                break;
                            } catch (InvocationTargetException e) {
                                //only log stack trace on last run
                                if (retries == numberOfRetries) {
                                    logger.error(String.format("error dispatching received value to consumer, giving up after run %d/%d", retries + 1, numberOfRetries), e);
                                } else {
                                    logger.warn(String.format("failed on run %d/%d, will retry: %s", retries + 1, numberOfRetries, e.toString()));
                                    sleepRetryBackoffMs();
                                }
                                retries++;
                            }
                        } while (!success && retries <= numberOfRetries);
                    } finally {
                        creationalContext.release();
                    }
                }
            }
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (isRunning()) {
                logger.trace("Unexpected WakeupException", e);
                throw e;
            }
        } catch (Throwable t) {
            logger.error("Unexpected fatal error", t);
        } finally {
            logger.info("Close the consumer.");
            metrics.consumerClosed();
            consumer.close();
        }
    }

    private void logSlowPoll(long pollStart, long pollEnd) {
        final long slowThreshold = 2000L;

        final long diffMs = pollEnd - pollStart;
        if (diffMs > slowThreshold) {
            logger.warn("slow kafka poll() took {} ms - longer than warning threshold of {} ms", diffMs, slowThreshold);
        }
    }

    private void sleepRetryBackoffMs() {
        try {
            Thread.sleep(retryBackoffMs);
        } catch (InterruptedException e) {
            // continue; shutdown is triggered by WakeupException only
        }
    }

    private void dispatchSinglePayload(ConsumerRecord<?, ?> record) throws IllegalAccessException, InvocationTargetException {
        logger.trace("dispatching payload {} to consumer", record.value());

        if (parameterTypes.length == 3) {
            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.key(), record.value(), record.headers());
        } else if (parameterTypes.length == 2) {
            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.key(), record.value());
        } else {
            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.value());
        }
    }

    private void dispatchCompletePayload(ConsumerRecords<?, ?> records) throws IllegalAccessException, InvocationTargetException {
        logger.trace("dispatching payload {} consumer records to consumer", records.count());
        annotatedListenerMethod.getJavaMember().invoke(consumerInstance, records);
    }

    private ConsumerMode getConsumerMode(Class<?>[] parameterTypes) {
        if (parameterTypes.length > 1) {
            return ConsumerMode.SINGLE;
        } else if (parameterTypes.length == 1) {
            if (parameterTypes[0].isAssignableFrom(ConsumerRecords.class)) {
                return ConsumerMode.ALL;
            }
            return ConsumerMode.SINGLE;
        } else {
            throw new IllegalArgumentException("Consumer methods must have at least one parameter.");
        }
    }

    /**
     * True when a consumer is running; otherwise false
     */
    private boolean isRunning() {
        return running.get();
    }

    /*
     * Shutdown hook which can be called from a separate thread.
     */
    public void shutdown() {
        logger.info("Shutting down the consumer.");
        running.set(Boolean.FALSE);
        consumer.wakeup();
    }

    private enum ConsumerMode {
        SINGLE, ALL;
    }

}
