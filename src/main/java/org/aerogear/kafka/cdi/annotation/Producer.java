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
package org.aerogear.kafka.cdi.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Simple annotation for POJO beans, to inject a Producer util implementation, with a given Kafka topic.
 */
@Inherited
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Producer {

    /**
     * Represents the kafka configuration property <code>linger.ms</code>. The default value for all producers can be configured with {@link KafkaConfig#defaultLingerMs()}.
     */
    int lingerMs() default -1;

    /**
     * Represents the kafka configuration property <code>retries</code>. The default value for all producers can be configured with {@link KafkaConfig#defaultProducerRetries()}.
     */
    int retries() default -1;

    /**
     * Represents the kafka configuration property <code>request.timeout.ms</code>. The default value for all producers can be configured with {@link KafkaConfig#defaultRequestTimeoutMs()}.
     */
    int requestTimeoutMs() default -1;

}
