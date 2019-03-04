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
package org.aerogear.kafka.cdi.extension;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VerySimpleEnvironmentResolver {

    //#{VARIABLE_NAME}
    private static final Pattern PATTERN = Pattern.compile("(#[{]([^}]+)[}])");

    private VerySimpleEnvironmentResolver() {
        // no-op
    }

    //#{KAFKA_SERVICE_HOST}:#{KAFKA_SERVICE_PORT} --> localhost:9090
    public static String resolveVariables(final String rawExpression) {
        Matcher m = PATTERN.matcher(rawExpression);
        String result = rawExpression;
        while (m.find()) {
            result = result.replace(m.group(1), resolve(m.group(2)));
        }
        return result;
    }

    private static String resolve(final String variable) {

        String value = System.getProperty(variable);
        if (value == null) {
            value = System.getenv(variable);
        }
        if (value == null) {
            throw new RuntimeException("Could not resolve: " + variable);
        }
        return value;
    }
}
