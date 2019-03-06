/*
 * Copyright 2019 NTT DATA, and individual contributors.
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

import org.jboss.weld.context.bound.BoundRequestContext;

import java.util.Map;

final class CdiRequestScopeUtils {

    /**
     * Start the request, providing a data store which will last the lifetime of the request
     */
    static void start(BoundRequestContext requestContext,
                      Map<String, Object> requestDataStore) {

        // Associate the store with the context and activate the context
        requestContext.associate(requestDataStore);
        requestContext.activate();
    }

    /**
     * End the request, providing the same data store as was used to start the request
     */
    static void end(BoundRequestContext requestContext,
                    Map<String, Object> requestDataStore) {

        try {
            // Invalidate the request (all bean instances will be scheduled for destruction)
            requestContext.invalidate();
            // Deactivate the request, causing all bean instances to be destroyed (as the context is invalid)
            requestContext.deactivate();

        } finally {
            // Ensure that whatever happens we dissociate to prevent any memory leaks
            requestContext.dissociate(requestDataStore);
        }
    }

    private CdiRequestScopeUtils() {
    }
}
