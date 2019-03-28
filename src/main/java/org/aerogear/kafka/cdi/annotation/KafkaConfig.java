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
 * Global configuration annotation. Only first occurrence will be used.
 */
@Inherited
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaConfig {
    String bootstrapServers();

    /**
     * Default number of retries dispatching calls. {code 0} means no retries.
     */
    int defaultConsumerRetries() default 0;

    /**
     * Default time in milli seconds to backoff between delivery retries.
     */
    int defaultConsumerRetryBackoffMs() default 100;

    /**
     * Represents the kafka configuration property <code>linger.ms</code>.
     */
    int defaultLingerMs() default -1;

    /**
     * Represents the kafka configuration property <code>fetch.max.wait.ms</code>.
     */
    int defaultFetchMaxWaitMs() default -1;

    /**
     * Represents the kafka configuration property <code>retries</code>.
     */
    int defaultProducerRetries() default 0;

    /**
     * Represents the kafka configuration property <code>request.timeout.ms</code> - for both consumer and producers.
     * <p>
     * Kafka's default value is 30000.
     * </p>
     * <p>
     * This should be larger than replica.lag.time.max.ms (a broker configuration)
     * to reduce the possibility of message duplication due to unnecessary producer retries.
     * The default for replica.lag.time.max.ms is 20000.
     * </p>
     */
    int defaultRequestTimeoutMs() default -1;
}
