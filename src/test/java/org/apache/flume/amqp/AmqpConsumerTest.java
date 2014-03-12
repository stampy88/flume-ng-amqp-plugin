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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author dave sinclair(stampy88@yahoo.com)
 */
public class AmqpConsumerTest extends BaseAmqpTest {
    private AmqpConsumer.Builder builder;
    private AmqpConsumer.BatchDeliveryListener batchDeliveryListener;
    private Channel channel;
    private QueueingConsumer queueingConsumer;

    @Before
    public void setup() {
        builder = AmqpConsumer.newBuilder();

        batchDeliveryListener = mock(AmqpConsumer.BatchDeliveryListener.class);
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);

        builder.setBatchDeliveryListener(batchDeliveryListener).
                setConnectionFactory(connectionFactory).
                setExchangeName(EXCHANGE_NAME).
                setExchangeType(Constants.AMQP.DIRECT_EXCHANGE).
                setDurableExchange(DURABLE_EXCHANGE).
                setQueueName(QUEUE_NAME).
                setAutoDeleteQueue(AUTO_DELETE_QUEUE).
                setDurableQueue(DURABLE_QUEUE).
                setExclusiveQueue(EXCLUSIVE_QUEUE).
                setBindings(BINDINGS).
                setPrefetchSize(PREFETCH_SIZE);

        channel = mock(Channel.class);

        queueingConsumer = mock(QueueingConsumer.class);
        when(queueingConsumer.getChannel()).thenReturn(channel);

    }

    @Test
    public void testProcessDeliveriesForChannel_AutoAckTrue_BatchSize2() throws Exception {
        Thread thread = threadInterruptedAfter(2);

        QueueingConsumer.Delivery delivery_1 = createDeliveryWithTag(1L);
        QueueingConsumer.Delivery delivery_2 = createDeliveryWithTag(2L);
        when(queueingConsumer.nextDelivery(AmqpConsumer.DELIVERY_TIMEOUT)).thenReturn(delivery_1).thenReturn(delivery_2);

        AmqpConsumer consumer = builder.setBatchSize(2).setAutoAck(true).build();
        consumer.processDeliveriesForConsumer(thread, queueingConsumer);

        verify(channel).basicConsume(QUEUE_NAME, true, queueingConsumer);
        verify(batchDeliveryListener).onBatchDelivery(anyList());
        verify(channel, never()).basicAck(2L, true);
    }

    @Test
    public void testProcessDeliveriesForChannel_AutoAckFalse_BatchSize1() throws Exception {
        Thread thread = threadInterruptedAfter(2);

        QueueingConsumer.Delivery delivery_1 = createDeliveryWithTag(1L);
        QueueingConsumer.Delivery delivery_2 = createDeliveryWithTag(2L);
        when(queueingConsumer.nextDelivery(AmqpConsumer.DELIVERY_TIMEOUT)).thenReturn(delivery_1).thenReturn(delivery_2);

        AmqpConsumer consumer = builder.setBatchSize(1).setAutoAck(false).build();
        consumer.processDeliveriesForConsumer(thread, queueingConsumer);

        verify(channel).basicConsume(QUEUE_NAME, false, queueingConsumer);
        verify(batchDeliveryListener, times(2)).onBatchDelivery(anyList());
        verify(channel).basicAck(1L, true);
        verify(channel).basicAck(2L, true);
    }

    @Test
    public void testProcessDeliveriesForChannel_AutoAckFalse_BatchSize3_Timeout() throws Exception {
        Thread thread = threadInterruptedAfter(3);

        QueueingConsumer.Delivery delivery_1 = createDeliveryWithTag(1L);
        QueueingConsumer.Delivery delivery_2 = createDeliveryWithTag(2L);
        when(queueingConsumer.nextDelivery(AmqpConsumer.DELIVERY_TIMEOUT)).thenReturn(delivery_1).thenReturn(delivery_2).thenReturn(null);

        AmqpConsumer consumer = builder.setBatchSize(3).setAutoAck(false).build();
        consumer.processDeliveriesForConsumer(thread, queueingConsumer);

        verify(channel).basicConsume(QUEUE_NAME, false, queueingConsumer);
        verify(batchDeliveryListener).onBatchDelivery(anyList());
        verify(channel).basicAck(2L, true);
    }

    @Test
    public void testProcessDeliveriesForChannel_NoDeliveries() throws Exception {
        Thread thread = threadInterruptedAfter(3);

        when(queueingConsumer.nextDelivery(AmqpConsumer.DELIVERY_TIMEOUT)).thenReturn(null).thenReturn(null).thenReturn(null);

        AmqpConsumer consumer = builder.setBatchSize(3).setAutoAck(false).build();
        consumer.processDeliveriesForConsumer(thread, queueingConsumer);

        verify(channel).basicConsume(QUEUE_NAME, false, queueingConsumer);
        verify(batchDeliveryListener, never()).onBatchDelivery(anyList());
        verify(channel, never()).basicAck(2L, true);
    }

    @Test
    public void testWaitToRetry() throws Exception {
        AmqpConsumer consumer = builder.build();

        long[] sleepTimes = new long[AmqpConsumer.MAX_POWER];
        for (int i = 0; i < sleepTimes.length; ++i) {
            sleepTimes[i] = (long) Math.pow(2, i) * AmqpConsumer.INITIAL_TIME_BETWEEN_RETRIES;
        }

        for (int i = 0; i < AmqpConsumer.MAX_POWER + 10; ++i) {
            AmqpConsumer.Sleeper sleeper = mock(AmqpConsumer.Sleeper.class);
            consumer.waitToRetry(sleeper, i);

            if (i < sleepTimes.length) {
                if (sleepTimes[i] < AmqpConsumer.MAX_TIME_BETWEEN_RETRIES) {
                    verify(sleeper).sleep(sleepTimes[i]);
                } else {
                    verify(sleeper).sleep(AmqpConsumer.MAX_TIME_BETWEEN_RETRIES);
                }

            } else {
                verify(sleeper).sleep(AmqpConsumer.MAX_TIME_BETWEEN_RETRIES);
            }
        }
    }

    @Test
    public void testDeclarationsForChannel_WithQueueNameAndBindings() throws Exception {
        AmqpConsumer consumer = builder.build();

        Channel channel = mock(Channel.class);
        consumer.declarationsForChannel(channel);

        verify(channel).exchangeDeclare(EXCHANGE_NAME, Constants.AMQP.DIRECT_EXCHANGE, DURABLE_EXCHANGE);
        verify(channel).basicQos(PREFETCH_SIZE);
        verify(channel).queueDeclare(QUEUE_NAME, DURABLE_QUEUE, EXCLUSIVE_QUEUE, AUTO_DELETE_QUEUE, null);
        verify(channel).queueBind(QUEUE_NAME, EXCHANGE_NAME, BINDING_KEY_1);
        verify(channel).queueBind(QUEUE_NAME, EXCHANGE_NAME, BINDING_KEY_2);
    }

    @Test
    public void testDeclarationsForChannel_WithQueueNameNoBindings() throws Exception {
        AmqpConsumer consumer = builder.setBindings(new String[]{}).build();

        Channel channel = mock(Channel.class);
        consumer.declarationsForChannel(channel);

        verify(channel).exchangeDeclare(EXCHANGE_NAME, Constants.AMQP.DIRECT_EXCHANGE, DURABLE_EXCHANGE);
        verify(channel).basicQos(PREFETCH_SIZE);
        verify(channel).queueDeclare(QUEUE_NAME, DURABLE_QUEUE, EXCLUSIVE_QUEUE, AUTO_DELETE_QUEUE, null);
        verify(channel).queueBind(QUEUE_NAME, EXCHANGE_NAME, Constants.AMQP.SERVER_GENERATED_QUEUE_NAME);
    }

    @Test
    public void testDeclarationsForChannel_NoQueueNameAndBindings() throws Exception {
        AmqpConsumer consumer = builder.setQueueName(null).build();

        AMQP.Queue.DeclareOk declareOk = mock(AMQP.Queue.DeclareOk.class);
        when(declareOk.getQueue()).thenReturn("queue");

        Channel channel = mock(Channel.class);
        when(channel.queueDeclare()).thenReturn(declareOk);
        consumer.declarationsForChannel(channel);

        verify(channel).exchangeDeclare(EXCHANGE_NAME, Constants.AMQP.DIRECT_EXCHANGE, DURABLE_EXCHANGE);
        verify(channel).basicQos(PREFETCH_SIZE);
        verify(channel).queueDeclare();
        verify(channel).queueBind("queue", EXCHANGE_NAME, BINDING_KEY_1);
        verify(channel).queueBind("queue", EXCHANGE_NAME, BINDING_KEY_2);
    }

    @Test
    public void testDeclarationsForChannel_NoQueueNameAndNoBindings() throws Exception {
        AmqpConsumer consumer = builder.setQueueName(null).setBindings(new String[]{}).build();

        AMQP.Queue.DeclareOk declareOk = mock(AMQP.Queue.DeclareOk.class);
        when(declareOk.getQueue()).thenReturn("queue");

        Channel channel = mock(Channel.class);
        when(channel.queueDeclare()).thenReturn(declareOk);
        consumer.declarationsForChannel(channel);

        verify(channel).exchangeDeclare(EXCHANGE_NAME, Constants.AMQP.DIRECT_EXCHANGE, DURABLE_EXCHANGE);
        verify(channel).basicQos(PREFETCH_SIZE);
        verify(channel).queueDeclare();
        verify(channel).queueBind("queue", EXCHANGE_NAME, Constants.AMQP.SERVER_GENERATED_QUEUE_NAME);
    }

    @Test
    public void testDeclarationsForChannel_NoPrefetch() throws Exception {
        AmqpConsumer consumer = builder.setPrefetchSize(0).build();

        Channel channel = mock(Channel.class);
        consumer.declarationsForChannel(channel);

        verify(channel).exchangeDeclare(EXCHANGE_NAME, Constants.AMQP.DIRECT_EXCHANGE, DURABLE_EXCHANGE);
        verify(channel, never()).basicQos(PREFETCH_SIZE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuilderSetBindings_Null() throws Exception {
        builder.setBindings(null);
    }

    @Test
    public void testBuilderSetBindings() throws Exception {
        String [] bindings = {"1", "2"};
        builder.setBindings(bindings);

        assertThat(builder.getBindings(), is(bindings));
        assertTrue(builder.getBindings() != bindings);
    }

    private Thread threadInterruptedAfter(int timesInvoked) {
        Thread thread = mock(Thread.class);

        if (timesInvoked == 1) {
            when(thread.isInterrupted()).thenReturn(false).thenReturn(true);

        } else {
            OngoingStubbing<Boolean> ongoingStubbing = when(thread.isInterrupted()).thenReturn(false);

            for (int i = 1; i < timesInvoked; ++i) {
                ongoingStubbing.thenReturn(false);
            }

            ongoingStubbing.thenReturn(true);
        }

        return thread;
    }
}


