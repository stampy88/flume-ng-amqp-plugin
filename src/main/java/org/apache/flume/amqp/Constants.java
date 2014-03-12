/**
 * Copyright 2014 Dave Sinclair
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.apache.flume.amqp;

import com.rabbitmq.client.ConnectionFactory;

/**
 * These are the constants that are used by multiple classes, both sinks and sources, in the flume plugin.
 *
 * @author dave sinclair(stampy88@yahoo.com)
 */
public class Constants {

    private Constants() {
    }

    public static class Defaults {
        private Defaults() {

        }

        private static final int UNBOUNDED_PREFETCH = 0;
        private static final String[] EMPTY_BINDINGS = {};

        public static final int PREFETCH_SIZE = UNBOUNDED_PREFETCH;
        public static final int BATCH_SIZE = 100;

        public static final boolean DURABLE_EXCHANGE = false;
        public static final boolean DURABLE_QUEUE = false;
        public static final boolean EXCLUSIVE_QUEUE = false;
        public static final boolean AUTO_DELETE_QUEUE = false;
        public static final boolean AUTO_ACK = false;
        public static final String[] BINDINGS = EMPTY_BINDINGS;
        public static final String EXCHANGE_TYPE = AMQP.DIRECT_EXCHANGE;

        public static final String HOST = ConnectionFactory.DEFAULT_HOST;
        public static final int PORT = ConnectionFactory.DEFAULT_AMQP_PORT;
        public static final String VIRTUAL_HOST = ConnectionFactory.DEFAULT_VHOST;
        public static final String USER_NAME = ConnectionFactory.DEFAULT_USER;
        public static final String PASSWORD = ConnectionFactory.DEFAULT_PASS;
        public static final int CONNECTION_TIMEOUT = ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT;
        public static final int REQUESTED_HEARTBEAT = ConnectionFactory.DEFAULT_HEARTBEAT;

    }

    public static class AMQP {

        private AMQP() {
        }

        /**
         * Exchange types as defined by AMQP specification
         */
        public static final String DIRECT_EXCHANGE = "direct";
        public static final String TOPIC_EXCHANGE = "topic";
        public static final String FANOUT_EXCHANGE = "fanout";
        public static final String HEADERS_EXCHANGE = "headers";
        public static final String NO_ROUTING_KEY = "";
        public static final String SERVER_GENERATED_QUEUE_NAME = "";
        /**
         * Per the AMQP specification - The server MUST implement the direct exchange type and MUST pre-declare
         * within each virtual host at least two direct exchanges: one named amq.direct, and one with no public name
         * that serves as the default exchange for Publish methods.
         * <p/>
         * The fanout exchange type, and a pre-declared exchange called amq.fanout, are mandatory.
         */
        public static final String SERVER_DEFAULT_EXCHANGE = "";
        public static final String SERVER_DIRECT_EXCHANGE = "amq.direct";
        public static final String SERVER_FANOUT_EXCHANGE = "amq.fanout";
    }

    /**
     * These are all of the names of the headers that are added to the Flume event from the
     * {@link org.apache.flume.amqp.AmqpSource.EventBatchDeliveryListener}
     */
    public static class Event {

        private Event() {
        }

        public static final String APP_ID = "appId";
        public static final String CONTENT_TYPE = "contentType";
        public static final String CONTENT_ENCODING = "contentEncoding";
        public static final String CORRELATION_ID = "correlationId";
        public static final String MESSAGE_ID = "messageId";
        public static final String EXPIRATION = "expiration";
        public static final String TYPE = "type";
        public static final String USER_ID = "userId";
        public static final String TIMESTAMP = "timestamp";
        public static final String SOURCE_ID = "sourceId";
    }
}
