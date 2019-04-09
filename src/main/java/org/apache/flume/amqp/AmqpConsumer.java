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

import com.google.common.annotations.VisibleForTesting;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.flume.ChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class is responsible for connecting to an AMQP Broker and consuming events from said broker. Each event
 * will be deliver to the specified {@link AmqpConsumer.BatchDeliveryListener}.
 * <p/>
 * The cancellation/shutdown policy for the consumer is to interrupt the thread that is running this consumer.
 *
 * @author dave sinclair(stampy88@yahoo.com)
 */
class AmqpConsumer implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpConsumer.class);

    /**
     * Maximum power we can raise 2^ * {@link #INITIAL_TIME_BETWEEN_RETRIES} to ensure roll over into negative
     */
    @VisibleForTesting
    static final int MAX_POWER = 53;

    @VisibleForTesting
    static final int INITIAL_TIME_BETWEEN_RETRIES = 1000;
    @VisibleForTesting
    static final int MAX_TIME_BETWEEN_RETRIES = INITIAL_TIME_BETWEEN_RETRIES * 60;
    @VisibleForTesting
    static final int DELIVERY_TIMEOUT = 1000;

    private final ConnectionFactory connectionFactory;

    /**
     * The {@link QueueingConsumer.Delivery} will be delivered to this instance
     *
     * @see #processDeliveriesForConsumer(Thread, com.rabbitmq.client.QueueingConsumer)
     */
    private final BatchDeliveryListener batchDeliveryListener;

    /**
     * Exchange level properties
     */
    private final String exchangeName;
    private final String exchangeType;
    /**
     * true if we are declaring a durable exchange (the exchange will survive a server restart)
     */
    private final boolean durableExchange;

    /**
     * Queue level properties
     */

    /**
     * if left unspecified, the server chooses a name and provides this to the client. Generally, when
     * applications share a message queue they agree on a message queue name beforehand, and when an
     * application needs a message queue for its own purposes, it lets the server provide a name.
     */
    private volatile String queueName;
    /**
     * if set, the message queue remains present and active when the server restarts. It may lose transient
     * messages if the server restarts.
     */
    private final boolean durableQueue;
    /**
     * if set, the queue belongs to the current connection only, and is deleted when the connection closes.
     */
    private final boolean exclusiveQueue;
    /**
     * true if we are declaring an autodelete queue (server will delete it when no longer in use)
     */
    private final boolean autoDeleteQueue;
    /**
     * bindings from {@link #exchangeName} to {@link #queueName}
     */
    private final String[] bindings;
    /**
     * true if the server should consider messages acknowledged once delivered; false if the server should expect
     * explicit acknowledgements
     */
    private final boolean autoAck;
    /**
     * Granularity at which to batch transfer events to the channel and if {@link #autoAck} is false, the size used to
     * acknowledge messages to the server.
     */
    private final int batchSize;

    /**
     * QOS Settings
     */
    private final int prefetchSize;

    private AmqpConsumer(Builder builder) {
        this.connectionFactory = builder.connectionFactory;
        this.batchDeliveryListener = builder.batchDeliveryListener;

        this.exchangeName = builder.exchangeName;
        this.exchangeType = builder.exchangeType;
        this.durableExchange = builder.durableExchange;

        this.queueName = builder.queueName;
        this.durableQueue = builder.durableQueue;
        this.exclusiveQueue = builder.exclusiveQueue;
        this.autoDeleteQueue = builder.autoDeleteQueue;
        this.autoAck = builder.autoAck;
        this.bindings = builder.bindings;

        this.prefetchSize = builder.prefetchSize;
        this.batchSize = builder.batchSize;
    }

    @Override
    public void run() {

        try {
            LOG.info("Starting consumer");
            consumeLoop();
        } catch (InterruptedException e) {
            // someone interrupted - take as shutdown message
            LOG.info("Received interrupt, shutting down consumer");

        } finally {
            LOG.info("Shutdown complete for consumer");
        }
    }

    /**
     * The main consume loop for this worker. This will sit in an infinite loop consuming messages from a
     * Channel until the executing thread is interrupted.
     *
     * @throws InterruptedException thrown if the current thread is interrupted from the
     *                              {@link #waitToRetry(AmqpConsumer.Sleeper, int)} method.
     */
    private void consumeLoop() throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        Channel channel = null;
        Connection connection = null;
        int numberOfConsecutiveConnectionErrors = 0;

        while (!currentThread.isInterrupted()) {
            try {
                connection = createConnection();
                // successfully connected, reset counter
                numberOfConsecutiveConnectionErrors = 0;

                channel = createChannel(connection);
                declarationsForChannel(channel);

                QueueingConsumer consumer = new QueueingConsumer(channel);
                processDeliveriesForConsumer(currentThread, consumer);
            } catch (InterruptedException e) {
                // NOTE - we need to catch InterruptedException before Exception to ensure we see the shutdown signal
                LOG.info("Received interrupt, shutting down consumer");
                // NEED to set the interrupt status on the thread to ensure the loop terminates
                currentThread.interrupt();

            } catch (IOException e) {
                LOG.info("IOException caught. Closing connection and waiting to reconnect", e);
                numberOfConsecutiveConnectionErrors++;

            } catch (Exception e) {
                // NOTE - this should never happen, but we don't want the thread to die because of an uncaught exception
                LOG.error("Consumer encountered an exception while processing the events", e);
            }

            // we need to take the interrupt status BEFORE closing because the close methods can alter the INTERRUPT state of the thread
            boolean wasInterrupted = currentThread.isInterrupted();

            closeChannelSilently(channel);
            closeConnectionSilently(connection);

            if (wasInterrupted) {
                currentThread.interrupt();
            }

            if (!currentThread.isInterrupted()) {
                waitToRetry(Sleeper.THREAD_SLEEPER, numberOfConsecutiveConnectionErrors);
            }
        }
    }

    @VisibleForTesting
    protected void processDeliveriesForConsumer(Thread currentThread, QueueingConsumer consumer) throws InterruptedException, ChannelException, IOException {
        Channel channel = consumer.getChannel();
        String consumerTag = channel.basicConsume(queueName, autoAck, consumer);
        LOG.info("Starting new consumer. Server generated {} as consumerTag", consumerTag);

        List<QueueingConsumer.Delivery> batch = new ArrayList<QueueingConsumer.Delivery>(batchSize);

        while (!currentThread.isInterrupted()) {
            QueueingConsumer.Delivery delivery = consumer.nextDelivery(DELIVERY_TIMEOUT);

            if (delivery == null && batch.size() > 0) {
                deliverBatch(channel, batch);

                batch.clear();

            } else if (delivery != null) {
                batch.add(delivery);

                if (batch.size() == batchSize) {
                    deliverBatch(channel, batch);

                    batch.clear();
                }
            }
        }
    }

    protected void deliverBatch(Channel channel, List<QueueingConsumer.Delivery> batch) throws IOException {
        batchDeliveryListener.onBatchDelivery(batch);

        if (!autoAck) {
            int lastItemIndex = batch.size() - 1;
            channel.basicAck(batch.get(lastItemIndex).getEnvelope().getDeliveryTag(), true);
        }
    }

    /**
     * This method declares the exchange, queue and bindings needed by this consumer.
     * The method returns the queue name that will be used by this consumer.
     *
     * @param channel channel used to issue AMQP commands
     * @return queue that will have messages consumed from
     * @throws IOException thrown if there is any communication exception
     */
    @VisibleForTesting
    protected String declarationsForChannel(Channel channel) throws IOException {
        // setup exchange, queue and binding
        if (prefetchSize > 0) {
            channel.basicQos(prefetchSize);
        }
        
        // if exchange is provided
        if (exchangeName != null){
            channel.exchangeDeclare(exchangeName, exchangeType, durableExchange);
        
            // named queue or server generated
            if (queueName == null) {
                queueName = channel.queueDeclare().getQueue();
            } else {
                channel.queueDeclare(queueName, durableQueue, exclusiveQueue, autoDeleteQueue, null);
            }

            if (bindings.length > 0) {
                // multiple bindings
                for (String binding : bindings) {
                    channel.queueBind(queueName, exchangeName, binding);
                }
            } else {
                // no binding given - this could be the case if it is a fanout exchange
                channel.queueBind(queueName, exchangeName, Constants.AMQP.SERVER_GENERATED_QUEUE_NAME);
            }
        }

        return queueName;
    }

    private Connection createConnection() throws IOException, TimeoutException {
        LOG.info("Connecting to {} ...", connectionFactory.getHost());
        Connection conn = connectionFactory.newConnection();
        LOG.info("Connected to {}", connectionFactory.getHost());

        return conn;
    }

    private Channel createChannel(Connection connection) throws IOException {
        LOG.info("Creating channel on connection {} ...", connection);
        Channel channel = connection.createChannel();
        LOG.info("Channel created on {}", connection);

        return channel;
    }

    private void closeChannelSilently(Channel channel) {
        if (channel != null) {
            try {
                LOG.info("Closing channel to {} ...", connectionFactory.getHost());
                channel.close();
                LOG.info("Channel to {} closed", connectionFactory.getHost());
            } catch (AlreadyClosedException e) {
                // we catch this specifically so we don't pollute the logs with already closed exceptions

            } catch (Exception e) {
                LOG.warn("Problem closing down channel", e);
            }
        }
    }

    private void closeConnectionSilently(Connection connection) {
        if (connection != null) {
            try {
                LOG.info("Closing connection to {} ...", connectionFactory.getHost());
                connection.close();
                LOG.info("Connection to {} closed", connectionFactory.getHost());
            } catch (AlreadyClosedException e) {
                // we catch this specifically so we don't pollute the logs with already closed exceptions

            } catch (Exception e) {
                LOG.warn("Problem closing down connection", e);
            }
        }
    }

    @VisibleForTesting
    protected void waitToRetry(Sleeper sleeper, int numberOfConsecutiveConnectionErrors) throws InterruptedException {
        long backOffTime = (long) Math.pow(2, Math.min(numberOfConsecutiveConnectionErrors, MAX_POWER)) * INITIAL_TIME_BETWEEN_RETRIES;

        // limit the possible wait time to MAX_TIME_BETWEEN_RETRIES
        long waitTime = backOffTime > MAX_TIME_BETWEEN_RETRIES ? MAX_TIME_BETWEEN_RETRIES : backOffTime;

        LOG.debug("Waiting {} milliseconds before retrying to connect...", waitTime);

        sleeper.sleep(waitTime);
    }

    @VisibleForTesting
    static interface Sleeper {
        void sleep(long millis) throws InterruptedException;

        static final Sleeper THREAD_SLEEPER = new Sleeper() {
            @Override
            public void sleep(long millis) throws InterruptedException {
                Thread.sleep(millis);
            }
        };
    }

    static Builder newBuilder() {
        return new Builder();
    }

    static class Builder {
        private static final int DEFAULT_BATCH_SIZE = 100;
        private static final String [] EMPTY_BINDINGS = {};

        private ConnectionFactory connectionFactory;
        private BatchDeliveryListener batchDeliveryListener;
        private String exchangeName;
        private String exchangeType;
        private boolean durableExchange;
        private String queueName;
        private boolean durableQueue;
        private boolean exclusiveQueue;
        private boolean autoDeleteQueue;
        private String[] bindings = EMPTY_BINDINGS;
        private int prefetchSize;
        private int batchSize = DEFAULT_BATCH_SIZE;
        public boolean autoAck;

        Builder setExchangeName(String exchangeName) {
            this.exchangeName = exchangeName;
            return this;
        }

        String getExchangeName() {
            return exchangeName;
        }

        Builder setExchangeType(String exchangeType) {
            this.exchangeType = exchangeType;
            return this;
        }

        String getExchangeType() {
            return exchangeType;
        }

        Builder setDurableExchange(boolean durableExchange) {
            this.durableExchange = durableExchange;
            return this;
        }

        boolean isDurableExchange() {
            return durableExchange;
        }

        Builder setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        String getQueueName() {
            return queueName;
        }

        Builder setDurableQueue(boolean durableQueue) {
            this.durableQueue = durableQueue;
            return this;
        }

        boolean isDurableQueue() {
            return durableQueue;
        }

        Builder setExclusiveQueue(boolean exclusiveQueue) {
            this.exclusiveQueue = exclusiveQueue;
            return this;
        }

        boolean isExclusiveQueue() {
            return exclusiveQueue;
        }

        Builder setAutoDeleteQueue(boolean autoDeleteQueue) {
            this.autoDeleteQueue = autoDeleteQueue;
            return this;
        }

        boolean isAutoDeleteQueue() {
            return autoDeleteQueue;
        }

        Builder setBindings(String[] bindings) {
            checkArgument(bindings != null, "bindings cannot be null");
            this.bindings = Arrays.copyOf(bindings, bindings.length);
            return this;
        }

        String[] getBindings() {
            return Arrays.copyOf(bindings, bindings.length);
        }

        Builder setBatchDeliveryListener(BatchDeliveryListener batchDeliveryListener) {
            this.batchDeliveryListener = batchDeliveryListener;
            return this;
        }

        Builder setConnectionFactory(ConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
            return this;
        }

        int getPrefetchSize() {
            return prefetchSize;
        }

        Builder setPrefetchSize(int prefetchSize) {
            checkArgument(prefetchSize >= 0, "prefetchSize cannot be negative");
            this.prefetchSize = prefetchSize;
            return this;
        }

        Builder setBatchSize(int batchSize) {
            checkArgument(batchSize > 0, "batchSize has to be greater than zero");
            this.batchSize = batchSize;
            return this;
        }

        int getBatchSize() {
            return batchSize;
        }

        Builder setAutoAck(boolean autoAck) {
            this.autoAck = autoAck;
            return this;
        }

        boolean isAutoAck() {
            return autoAck;
        }

        public AmqpConsumer build() {
            checkArgument(exchangeName != null || queueName != null, "exchangeName and queueName cannot both be null");
            checkArgument(batchDeliveryListener != null, "batchDeliveryListener cannot be null");
            checkArgument(connectionFactory != null, "connectionFactory cannot be null");
            return new AmqpConsumer(this);
        }
    }

    static interface BatchDeliveryListener {
        void onBatchDelivery(List<QueueingConsumer.Delivery> batch) throws ChannelException;
    }
}
