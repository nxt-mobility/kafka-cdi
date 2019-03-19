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
import javax.enterprise.inject.spi.AnnotatedMethod;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.CDI;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

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

    private Class<?> consumerKeyType(final Class<?> defaultKeyType, final AnnotatedMethod annotatedMethod) {

        if (annotatedMethod.getJavaMember().getParameterTypes().length >= 2) {
            return annotatedMethod.getJavaMember().getParameterTypes()[0];
        } else {
            return defaultKeyType;
        }
    }

    private Class<?> consumerValueType(final AnnotatedMethod annotatedMethod) {

        if (annotatedMethod.getJavaMember().getParameterTypes().length >= 2) {
            return annotatedMethod.getJavaMember().getParameterTypes()[1];
        } else {
            return annotatedMethod.getJavaMember().getParameterTypes()[0];
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

        numberOfRetries = kafkaConfig.consumerRetries();

        final String groupId = VerySimpleEnvironmentResolver.resolveVariables(consumerAnnotation.groupId());
        final Class<?> recordKeyType = consumerAnnotation.keyType();

        this.annotatedListenerMethod = annotatedMethod;

        final Class<?> keyTypeClass = consumerKeyType(recordKeyType, annotatedMethod);
        final Class<?> valTypeClass = consumerValueType(annotatedMethod);

        properties.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(GROUP_ID_CONFIG, groupId);
        properties.put(AUTO_OFFSET_RESET_CONFIG, consumerAnnotation.offset());
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, CafdiSerdes.serdeFrom(keyTypeClass).deserializer().getClass());
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, CafdiSerdes.serdeFrom(valTypeClass).deserializer().getClass());

        createKafkaConsumer(keyTypeClass, valTypeClass, properties);
        this.consumerRebalanceListener = createConsumerRebalanceListener(consumerAnnotation.consumerRebalanceListener());

        final Set<Bean<?>> beans = beanManager.getBeans(annotatedListenerMethod.getJavaMember().getDeclaringClass());
        final Bean<?> propertyResolverBean = beanManager.resolve(beans);
        final CreationalContext<?> creationalContext = beanManager.createCreationalContext(propertyResolverBean);
        final Type consumerTpye = annotatedListenerMethod.getJavaMember().getDeclaringClass();

        consumerInstance = beanManager.getReference(propertyResolverBean, consumerTpye, creationalContext);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics, consumerRebalanceListener);
            logger.info("subscribed to {}", topics);
            while (isRunning()) {
                final ConsumerRecords<?, ?> records = consumer.poll(100);
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
                                    dispatchPayload(record);
                                    success = true;
                                } finally {
                                    CdiRequestScopeUtils.end(boundRequestContext, requestDataStore);
                                }
                                logger.trace("dispatched payload {} to consumer", record.value());
                            } catch (IllegalAccessException e) {
                                logger.error("error dispatching received value to consumer", e);
                                break;
                            } catch (InvocationTargetException e) {
                                //only log stack trace on last run
                                if (retries == numberOfRetries) {
                                    logger.error(String.format("error dispatching received value to consumer, giving up after run %d/%d", retries + 1, numberOfRetries), e);
                                } else {
                                    logger.warn(String.format("failed on run %d/%d, will retry: %s", retries + 1, numberOfRetries, e.toString()));
                                }
                                retries++;
                            }
                        } while (!success && retries <= numberOfRetries);
                    } finally {
                        creationalContext.release();
                    }
                }
            }
        } catch (SerializationException e) {
            logger.warn("Consumer exception", e);
            throw e;
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (isRunning()) {
                logger.trace("Exception", e);
                throw e;
            }
        } catch (Exception e) {
            logger.error("Exception", e);
        } finally {
            logger.info("Close the consumer.");
            consumer.close();
        }
    }

    private void dispatchPayload(ConsumerRecord<?, ?> record) throws IllegalAccessException, InvocationTargetException {
        logger.trace("dispatching payload {} to consumer", record.value());

        final Class<?>[] parameterTypes = annotatedListenerMethod.getJavaMember().getParameterTypes();

        if (parameterTypes.length == 3) {
            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.key(), record.value(), record.headers());
        } else if (parameterTypes.length == 2) {
            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.key(), record.value());

        } else {
            annotatedListenerMethod.getJavaMember().invoke(consumerInstance, record.value());
        }
    }

    /**
     * True when a consumer is running; otherwise false
     */
    public boolean isRunning() {
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

}
