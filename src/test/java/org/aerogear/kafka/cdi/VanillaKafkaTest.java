/**
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
package org.aerogear.kafka.cdi;

import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.aerogear.kafka.cdi.tests.KafkaClusterTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Arquillian.class)
public class VanillaKafkaTest extends KafkaClusterTestBase {

    private Logger logger = LoggerFactory.getLogger(VanillaKafkaTest.class);

    @Deployment
    public static JavaArchive createDeployment() {

        return AbstractTestBase.createFrameworkDeployment();
    }


    @Test
    public void vanillaKafka() throws IOException, InterruptedException {

        final String topicName = "vanillaKafka";
        final String producerId = topicName;
        final String consumerId = topicName;
        kafkaCluster().createTopic(topicName, 1, 1);

        final Properties consumerProperties = kafkaCluster().useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(consumerProperties);
        consumer.subscribe(Arrays.asList(topicName));

        final Properties producerProperties = kafkaCluster().useTo().getProducerProperties(producerId);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer = new KafkaProducer<>(producerProperties);

        producer.send(new ProducerRecord(topicName, "Hello"));
        producer.send(new ProducerRecord(topicName, "Hello"));

        final CountDownLatch latch = new CountDownLatch(2);
        final ConsumerRecords<String, String> records = consumer.poll(1000);
        for (final ConsumerRecord<String, String> record : records) {
            assertThat(record.value()).isEqualTo("Hello");
            latch.countDown();
        }

        final boolean done = latch.await(2, TimeUnit.SECONDS);
        assertThat(done).isTrue();
    }
}
