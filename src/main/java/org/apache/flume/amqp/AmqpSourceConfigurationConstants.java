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

/**
 * These are the configurable parameters exposed by the {@link AmqpSource} class. See declarations for explanations
 * of each. This class is separate from the other {@link Constants} class to follow Flume conventions.
 *
 * @author dave sinclair(stampy88@yahoo.com)
 */
class AmqpSourceConfigurationConstants {
    /**
     * This is the name of the exchange the source will be consuming messages from. This is a required field.
     */
    public static final String EXCHANGE_NAME = "exchangeName";

    /**
     * Host name of the Server that hosts the AMQP Broker. It defaults to {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_HOST}
     */
    public static final String HOST = "host";

    /**
     * Port of the server that hosts the AMQP Broker. It defaults to {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_AMQP_PORT}
     */
    public static final String PORT = "port";

    /**
     * The virtual host to use when connecting to the broker. It defaults to {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_VHOST}
     */
    public static final String VIRTUAL_HOST = "virtualHost";

    /**
     * The AMQP user name to use when connecting to the broker. It defaults to {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_USER}
     */
    public static final String USER_NAME = "userName";

    /**
     * The AMQP password to use when connecting to the broker. It defaults to {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_PASS}
     */
    public static final String PASSWORD = "password";

    /**
     * The AMQP Exchange type. Valid types are direct, fanout, topic, and header. Defaults to direct when not specified.
     */
    public static final String EXCHANGE_TYPE = "exchangeType";

    /**
     * True if we are declaring a durable exchange (the exchange will survive a server restart). Defaults to false.
     */
    public static final String DURABLE_EXCHANGE = "durableExchange";

    /**
     * If left unspecified, the server chooses a name and provides this to the client. Generally, when applications
     * share a message queue they agree on a message queue name beforehand, and when an application needs a message
     * queue for its own purposes, it lets the server provide a name.
     */
    public static final String QUEUE_NAME = "queueName";

    /**
     * If true, the message queue remains present and active when the server restarts. It may lose transient
     * messages if the server restarts.
     */
    public static final String DURABLE_QUEUE = "durableQueue";

    /**
     * If true, the queue belongs to the current connection only, and is deleted when the connection closes.
     */
    public static final String EXCLUSIVE_QUEUE = "exclusiveQueue";

    /**
     * True if we are declaring an autodelete queue (server will delete it when no longer in use).
     */
    public static final String AUTO_DELETE_QUEUE = "autoDeleteQueue";

    /**
     * If this field is set the server does not expect acknowledgements for messages. That is, when a message is
     * delivered to the client the server assumes the delivery will succeed and immediately dequeues it.
     * This functionality may increase performance but at the cost of reliability.
     * Messages can get lost if a client dies before they are delivered to the application.
     */
    public static final String AUTO_ACK = "autoAck";

    /**
     * The client can request that messages be sent in advance so that when the client finishes processing a message,
     * the following message is already held locally, rather than needing to be sent down the channel. Prefetching
     * gives a performance improvement. The server will send a message in advance if it is equal to or smaller in
     * size than the available prefetch size (and also falls into other prefetch limits). May be set to zero,
     * meaning "no specific limit", although other prefetch limits may still apply.
     * The prefetch-size is ignored if the no-ack option is set.
     */
    public static final String PREFETCH_SIZE = "prefetchSize";

    /**
     * Space separated list of strings that will be used to bind the queue to the exchange. This is not required for
     * certain exchange types.
     */
    public static final String BINDINGS = "bindings";

    /**
     * If true, the timestamp for the Flume event will be based on the timestamp from the AMQP message.
     */
    public static final String USE_MESSAGE_TIMESTAMP = "useMessageTimestamp";

    /**
     * Connection establishment timeout in milliseconds; zero for infinite. Defaults to
     * {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_CONNECTION_TIMEOUT}
     */
    public static final String CONNECTION_TIMEOUT = "connectionTimeout";

    /**
     * The initially requested heartbeat interval, in seconds; zero for none. Defaults to
     * {@link com.rabbitmq.client.ConnectionFactory#DEFAULT_HEARTBEAT}
     */
    public static final String REQUEST_HEARTBEAT = "requestedHeartbeat";

    /**
     * This property has dual purposes. If the source is not in auto-ack mode, then this will be the number of messages
     * to buffer before sending an ack to the server.
     * <p/>
     * Regardless of the ack mode, this property controls the number of events that are transferred to a Flume
     * channel at a time.
     */
    public static final String BATCH_SIZE = "batchSize";
}
