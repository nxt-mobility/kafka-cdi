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
package org.aerogear.kafka.cdi.beans.mock;

import org.aerogear.kafka.cdi.annotation.ForTopic;
import org.mockito.Mockito;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;

import static org.aerogear.kafka.cdi.ReceiveMessageFromInjectedServiceTest.*;

@ApplicationScoped
public class MockProvider {

    private MessageReceiver simpleTopicReceiver = Mockito.mock(MessageReceiver.class);
    private MessageReceiver extendedTopicReceiver = Mockito.mock(MessageReceiver.class);
    private MessageReceiver protoTopicReceiver = Mockito.mock(MessageReceiver.class);

    @Produces
    @ForTopic(SIMPLE_PRODUCER_TOPIC_NAME)
    public MessageReceiver simpleReceiver() {
        return simpleTopicReceiver;
    }

    @Produces
    @ForTopic(EXTENDED_PRODUCER_TOPIC_NAME)
    public MessageReceiver extendedReceiver() {
        return extendedTopicReceiver;
    }

    @Produces
    @ForTopic(PROTO_PRODUCER_TOPIC_NAME)
    public MessageReceiver protoReceiver() {
        return protoTopicReceiver;
    }
}
