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
package org.aerogear.kafka.cdi.tests;

import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

public abstract class KafkaClusterTestBase extends AbstractTestBase {

    protected KafkaConsumer consumer;
    protected KafkaProducer producer;

    private static File dataDir;
    private static KafkaCluster kafkaCluster;

    protected static KafkaCluster kafkaCluster() {
        if (kafkaCluster != null) {
            return kafkaCluster;
        }

        dataDir = Testing.Files.createTestingDirectory("cluster");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .withPorts(2282, 9098);
        try {
            kafkaCluster = kafkaCluster().addBrokers(1).startup();
        } catch (IOException e) {
            throw new RuntimeException("Failed to startup broker", e);
        }

        return kafkaCluster;
    }

    @AfterClass
    public static void tearDown() {
        if (kafkaCluster != null) {
            kafkaCluster.shutdown();
            kafkaCluster = null;
            boolean delete = dataDir.delete();
            // If files are still locked and a test fails: delete on exit to allow subsequent test execution
            if(!delete) {
                dataDir.deleteOnExit();
            }
        }
    }

    @After
    public void afterTest() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        if (consumer != null) {
            consumer.close();
            consumer = null;
        }


    }
}