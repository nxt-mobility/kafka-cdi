/**
 * Copyright 2017 Red Hat, Inc, and individual contributors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.aerogear.kafka.cdi;

import org.aerogear.kafka.cdi.annotation.ForTopic;
import org.aerogear.kafka.cdi.beans.KafkaService;
import org.aerogear.kafka.cdi.beans.ProtoUsingKafkaService;
import org.aerogear.kafka.cdi.beans.mock.MessageReceiver;
import org.aerogear.kafka.cdi.beans.mock.MockProvider;
import org.aerogear.kafka.cdi.proto.AddressBookProtos;
import org.aerogear.kafka.cdi.tests.AbstractTestBase;
import org.aerogear.kafka.cdi.tests.KafkaClusterTestBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;


@RunWith(Arquillian.class)
public class ReceiveMessageFromInjectedServiceTest extends KafkaClusterTestBase {

    public static final String SIMPLE_PRODUCER_TOPIC_NAME = "ServiceInjectionTest.SimpleProducer";
    public static final String PROTO_PRODUCER_TOPIC_NAME = "ServiceInjectionTest.ProtoProducer";
    public static final String EXTENDED_PRODUCER_TOPIC_NAME = "ServiceInjectionTest.ExtendedProducer";

    private static final Logger logger = LoggerFactory.getLogger(ReceiveMessageFromInjectedServiceTest.class);

    @Deployment
    public static JavaArchive createDeployment() {
        logger.info("creating deployment");
        createTopics();

        return AbstractTestBase.createFrameworkDeployment()
                .addPackage(KafkaService.class.getPackage())
                .addPackage(MockProvider.class.getPackage());
    }

    @Inject
    private KafkaService service;

    @Inject
    private ProtoUsingKafkaService protoService;

    private static void createTopics() {
        kafkaCluster().createTopics(SIMPLE_PRODUCER_TOPIC_NAME, EXTENDED_PRODUCER_TOPIC_NAME, PROTO_PRODUCER_TOPIC_NAME);
    }

    @Test
    public void testSendAndReceive(@ForTopic(SIMPLE_PRODUCER_TOPIC_NAME) MessageReceiver receiver) throws Exception {

        final String consumerId = SIMPLE_PRODUCER_TOPIC_NAME;

        Thread.sleep(1000);
        service.sendMessage();

        Properties cconfig = kafkaCluster().useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(SIMPLE_PRODUCER_TOPIC_NAME));

        boolean loop = true;

        while (loop) {

            final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<String, String> record : records) {
                logger.trace("In polling loop, we got {}", record.value());
                assertThat(record.value(), equalTo("This is only a test"));
                loop = false;
            }
        }

        Thread.sleep(2000);

        Mockito.verify(receiver, Mockito.times(1)).ack();
    }

    @Test
    public void testSendAndReceiveProto(@ForTopic(PROTO_PRODUCER_TOPIC_NAME) MessageReceiver receiver) throws Exception {
        Thread.sleep(1000);

        ArgumentCaptor<AddressBookProtos.Person> personCaptor = ArgumentCaptor.forClass(AddressBookProtos.Person.class);
        protoService.sendMessage();
        Thread.sleep(2000);
        Mockito.verify(receiver, Mockito.times(2)).ack(Mockito.any(), personCaptor.capture(), Mockito.any());
        assertEquals("Franz Kafka", personCaptor.getValue().getName());
    }

    @Test
    public void testSendAndReceiveWithHeader(@ForTopic(EXTENDED_PRODUCER_TOPIC_NAME) MessageReceiver receiver) throws Exception {
        final String consumerId = EXTENDED_PRODUCER_TOPIC_NAME;

        Thread.sleep(1000);
        service.sendMessageWithHeader();

        Properties cconfig = kafkaCluster().useTo().getConsumerProperties(consumerId, consumerId, OffsetResetStrategy.EARLIEST);
        cconfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cconfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer = new KafkaConsumer(cconfig);

        consumer.subscribe(Arrays.asList(EXTENDED_PRODUCER_TOPIC_NAME));

        boolean loop = true;

        while (loop) {

            final ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (final ConsumerRecord<String, String> record : records) {
                logger.trace("In polling loop, we got {}", record.value());
                assertThat(record.value(), equalTo("This is only a second test"));
                final Headers headers = record.headers();
                final Header header = headers.lastHeader("header.key");
                assertThat(header.key(), equalTo("header.key"));
                loop = false;
            }
        }

        Thread.sleep(2000);

        List<Header> headersList = Arrays.asList(new RecordHeader("header.key", "header.value".getBytes(Charset.forName("UTF-8"))));
        RecordHeaders expectedHeaders = new RecordHeaders(headersList);

        Mockito.verify(receiver, Mockito.times(1)).ack(42, "This is only a second test", expectedHeaders);
    }
}
