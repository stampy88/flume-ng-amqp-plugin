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
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.apache.flume.ChannelException;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.SystemClock;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractEventDrivenSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Source} that receives events from an AMQP Broker and constructs Flume {@link Event} from said AMQP
 * events.
 *
 * @author dave sinclair(stampy88@yahoo.com)
 * @see AmqpConsumer
 * @see AmqpSourceConfigurationConstants
 */
public class AmqpSource extends AbstractEventDrivenSource {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpSource.class);

    /**
     * This is used when creating the name of the thread to run the {@link AmqpConsumer}.
     */
    private static final AtomicInteger SOURCE_ID = new AtomicInteger();

    private SourceCounter sourceCounter;
    private Thread runner;

    @Override
    protected synchronized void doConfigure(Context context) throws FlumeException {
        LOG.info("Configuring AMQP Source...");

        sourceCounter = new SourceCounter(getName());

        boolean useMessageTimestamp = context.getBoolean(AmqpSourceConfigurationConstants.USE_MESSAGE_TIMESTAMP, false);
        int sourceId = SOURCE_ID.getAndIncrement();

        EventBatchDeliveryListener deliverListener = new EventBatchDeliveryListener(this, sourceCounter, sourceId, useMessageTimestamp);

        ConnectionFactory connectionFactory = createConnectionFactoryFrom(context);
        AmqpConsumer.Builder consumerBuilder = createConsumerBuilderFrom(context);

        AmqpConsumer consumer = consumerBuilder.setConnectionFactory(connectionFactory)
                .setBatchDeliveryListener(deliverListener)
                .build();

        this.runner = new Thread(consumer, "amqp-source-" + sourceId);

        LOG.info("Configuring AMQP Source...complete");
    }

    @Override
    protected synchronized void doStart() throws FlumeException {
        LOG.info("Starting AMQP Source...");

        sourceCounter.start();
        runner.start();

        LOG.info("Starting AMQP Source...complete");
    }

    @Override
    protected synchronized void doStop() throws FlumeException {
        LOG.info("Stopping AMQP Source...");

        runner.interrupt();
        try {
            runner.join();
        } catch (InterruptedException e) {
            // if we are interrupted, someone is telling us to stop right now
        }
        sourceCounter.stop();

        LOG.info("Stopping AMQP Source...complete");
    }

    @VisibleForTesting
    static AmqpConsumer.Builder createConsumerBuilderFrom(Context context) {
        // only exchange name is required
        Configurables.ensureRequiredNonNull(context, AmqpSourceConfigurationConstants.EXCHANGE_NAME);

        AmqpConsumer.Builder builder = AmqpConsumer.newBuilder()
                .setExchangeName(context.getString(AmqpSourceConfigurationConstants.EXCHANGE_NAME))
                .setExchangeType(context.getString(AmqpSourceConfigurationConstants.EXCHANGE_TYPE, Constants.Defaults.EXCHANGE_TYPE))
                .setDurableExchange(context.getBoolean(AmqpSourceConfigurationConstants.DURABLE_EXCHANGE, Constants.Defaults.DURABLE_EXCHANGE))
                .setQueueName(context.getString(AmqpSourceConfigurationConstants.QUEUE_NAME))
                .setDurableQueue(context.getBoolean(AmqpSourceConfigurationConstants.DURABLE_QUEUE, Constants.Defaults.DURABLE_QUEUE))
                .setExclusiveQueue(context.getBoolean(AmqpSourceConfigurationConstants.EXCLUSIVE_QUEUE, Constants.Defaults.EXCLUSIVE_QUEUE))
                .setAutoDeleteQueue(context.getBoolean(AmqpSourceConfigurationConstants.AUTO_DELETE_QUEUE, Constants.Defaults.AUTO_DELETE_QUEUE))
                .setAutoAck(context.getBoolean(AmqpSourceConfigurationConstants.AUTO_ACK, Constants.Defaults.AUTO_ACK))
                .setPrefetchSize(context.getInteger(AmqpSourceConfigurationConstants.PREFETCH_SIZE, Constants.Defaults.PREFETCH_SIZE))
                .setBatchSize(context.getInteger(AmqpSourceConfigurationConstants.BATCH_SIZE, Constants.Defaults.BATCH_SIZE));

        String bindingsList = context.getString(AmqpSourceConfigurationConstants.BINDINGS);
        String[] bindings = Constants.Defaults.BINDINGS;
        if (bindingsList != null) {
            bindings = bindingsList.split(("\\s+"));
        }

        builder.setBindings(bindings);

        return builder;
    }

    @VisibleForTesting
    static ConnectionFactory createConnectionFactoryFrom(Context context) {
        String host = context.getString(AmqpSourceConfigurationConstants.HOST, Constants.Defaults.HOST);
        int port = context.getInteger(AmqpSourceConfigurationConstants.PORT, Constants.Defaults.PORT);
        String virtualHost = context.getString(AmqpSourceConfigurationConstants.VIRTUAL_HOST, Constants.Defaults.VIRTUAL_HOST);
        String userName = context.getString(AmqpSourceConfigurationConstants.USER_NAME, Constants.Defaults.USER_NAME);
        String password = context.getString(AmqpSourceConfigurationConstants.PASSWORD, Constants.Defaults.PASSWORD);
        int connectionTimeout = context.getInteger(AmqpSourceConfigurationConstants.CONNECTION_TIMEOUT, Constants.Defaults.CONNECTION_TIMEOUT);
        int requestHeartbeat = context.getInteger(AmqpSourceConfigurationConstants.REQUEST_HEARTBEAT, Constants.Defaults.REQUESTED_HEARTBEAT);

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connectionFactory.setPort(port);
        connectionFactory.setVirtualHost(virtualHost);
        connectionFactory.setUsername(userName);
        connectionFactory.setPassword(password);
        connectionFactory.setConnectionTimeout(connectionTimeout);
        connectionFactory.setRequestedHeartbeat(requestHeartbeat);

        return connectionFactory;
    }

    @VisibleForTesting
    static class EventBatchDeliveryListener implements AmqpConsumer.BatchDeliveryListener {
        private final boolean useMessageTimestamp;
        private final Clock clock;
        private final Source source;
        private final SourceCounter sourceCounter;
        private final String sourceId;

        private EventBatchDeliveryListener(Source source, SourceCounter sourceCounter, int sourceId, boolean useMessageTimestamp) {
            this(source, sourceCounter, sourceId, useMessageTimestamp, new SystemClock());
        }

        @VisibleForTesting
        EventBatchDeliveryListener(Source source, SourceCounter sourceCounter, int sourceId, boolean useMessageTimestamp, Clock clock) {
            this.source = source;
            this.sourceCounter = sourceCounter;
            this.sourceId = String.valueOf(sourceId);
            this.useMessageTimestamp = useMessageTimestamp;
            this.clock = clock;
        }

        @Override
        public void onBatchDelivery(List<QueueingConsumer.Delivery> batch) throws ChannelException {
            List<Event> events = new ArrayList<Event>(batch.size());

            for (QueueingConsumer.Delivery delivery : batch) {
                try {
                    Event event = createEventFrom(delivery);

                    events.add(event);
                } catch (Exception e) {
                    LOG.error("Error occurred while creating event from delivery", e);
                }
            }

            sourceCounter.incrementAppendBatchReceivedCount();
            // NOTE this is batch size, not event size. We want to have an accurate count of the number of raw events
            // we received
            sourceCounter.addToEventReceivedCount(batch.size());

            // this could end up being zero if there were problems creating the events from the delivery
            if (events.size() > 0) {
                // we handle the event outside of the try/catch because we want channel exceptions to be propagated up
                source.getChannelProcessor().processEventBatch(events);

                sourceCounter.addToEventAcceptedCount(events.size());
                sourceCounter.incrementAppendBatchAcceptedCount();
            }
        }

        /**
         * Creates a new flume {@link org.apache.flume.Event} from the message delivery. Properties from the
         * {@link QueueingConsumer.Delivery} will be put on the new event. Note that if the {@link #useMessageTimestamp} is
         * true and there is a timestamp on the message, the newly created flume event will have the message's timestamp.
         *
         * @param delivery message that is being processed
         * @return new flume event
         */
        @VisibleForTesting
        Event createEventFrom(QueueingConsumer.Delivery delivery) {
            long timeInMS = -1;

            Map<String, String> eventHeaders = new HashMap<String, String>();

            AMQP.BasicProperties msgProperties = delivery.getProperties();

            if (msgProperties != null) {
                if (useMessageTimestamp) {
                    if (msgProperties.getTimestamp() != null) {
                        timeInMS = msgProperties.getTimestamp().getTime();
                    }
                }
                if (msgProperties.getAppId() != null) {
                    eventHeaders.put(Constants.Event.APP_ID, msgProperties.getAppId());
                }
                if (msgProperties.getContentType() != null) {
                    eventHeaders.put(Constants.Event.CONTENT_TYPE, msgProperties.getContentType());
                }
                if (msgProperties.getContentEncoding() != null) {
                    eventHeaders.put(Constants.Event.CONTENT_ENCODING, msgProperties.getContentEncoding());
                }
                if (msgProperties.getCorrelationId() != null) {
                    eventHeaders.put(Constants.Event.CORRELATION_ID, msgProperties.getCorrelationId());
                }
                if (msgProperties.getMessageId() != null) {
                    eventHeaders.put(Constants.Event.MESSAGE_ID, msgProperties.getMessageId());
                }
                if (msgProperties.getExpiration() != null) {
                    eventHeaders.put(Constants.Event.EXPIRATION, msgProperties.getExpiration());
                }
                if (msgProperties.getType() != null) {
                    eventHeaders.put(Constants.Event.TYPE, msgProperties.getType());
                }
                if (msgProperties.getUserId() != null) {
                    eventHeaders.put(Constants.Event.USER_ID, msgProperties.getUserId());
                }
                Map<String, Object> headers = msgProperties.getHeaders();
                if (headers != null) {
                    for (String key : headers.keySet()) {
                        if (headers.get(key) != null) {
                            eventHeaders.put(key, String.valueOf(headers.get(key)));
                        }
                    }
                }
            }

            if (timeInMS == -1) {
                timeInMS = clock.currentTimeMillis();
            }

            eventHeaders.put(Constants.Event.TIMESTAMP, String.valueOf(timeInMS));
            eventHeaders.put(Constants.Event.SOURCE_ID, sourceId);

            return EventBuilder.withBody(delivery.getBody(), eventHeaders);
        }
    }
}
