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

import com.rabbitmq.client.QueueingConsumer;
import org.apache.flume.Clock;
import org.apache.flume.Event;
import org.apache.flume.Source;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.instrumentation.SourceCounter;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

/**
 * @author dave sinclair(stampy88@yahoo.com)
 */
public class EventDeliveryListenerTest extends BaseAmqpTest {

    private static final DateTime SYSTEM_DATE = DateTime.now();
    private static final int SOURCE_ID = 55;

    private Source source;
    private ChannelProcessor channelProcessor;
    private SourceCounter sourceCounter;
    private Clock clock;
    private AmqpSource.EventBatchDeliveryListener listener;

    @Before
    public void setup() {
        channelProcessor = mock(ChannelProcessor.class);
        source = mock(Source.class);
        when(source.getChannelProcessor()).thenReturn(channelProcessor);

        sourceCounter = mock(SourceCounter.class);
        clock = mock(Clock.class);
        when(clock.currentTimeMillis()).thenReturn(SYSTEM_DATE.getMillis());

        listener = new AmqpSource.EventBatchDeliveryListener(source, sourceCounter, SOURCE_ID, true, clock);
    }

    @Test
    public void testOnBatchDelivery() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        listener.onBatchDelivery(Arrays.asList(delivery));

        verify(sourceCounter).incrementAppendBatchReceivedCount();
        verify(sourceCounter).addToEventReceivedCount(1);
        verify(channelProcessor).processEventBatch(listOfSize(1));

        verify(sourceCounter).addToEventAcceptedCount(1);
        verify(sourceCounter).incrementAppendBatchAcceptedCount();
    }

    @Test
    public void testOnBatchDelivery_Exception() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getBody()).thenThrow(new RuntimeException("fail"));

        listener.onBatchDelivery(Arrays.asList(delivery));

        verify(sourceCounter).incrementAppendBatchReceivedCount();
        verify(sourceCounter).addToEventReceivedCount(1);

        verifyZeroInteractions(channelProcessor);
        verify(sourceCounter, never()).addToEventAcceptedCount(1);
        verify(sourceCounter, never()).incrementAppendBatchAcceptedCount();
    }

    @Test
    public void testCreateEventFrom_IncludeMessageTimstamp() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getBody(), is(BODY));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.TIMESTAMP, String.valueOf(MESSAGE_DATE.getTime())));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.APP_ID, APP_ID));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.CONTENT_TYPE, CONTENT_TYPE));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.CONTENT_ENCODING, CONTENT_ENCODING));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.CORRELATION_ID, CORRELATION_ID));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.MESSAGE_ID, MESSAGE_ID));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.EXPIRATION, EXPIRATION));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.TYPE, TYPE));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.USER_ID, USER_ID));
        assertThat(event.getHeaders(), hasEntry(Constants.Event.SOURCE_ID, String.valueOf(SOURCE_ID)));
        assertThat(event.getHeaders(), hasEntry(KEY_1, VALUE_1));
        assertThat(event.getHeaders(), hasEntry(KEY_2, VALUE_2));
    }

    @Test
    public void testCreateEventFrom_IncludeSystemTimstamp() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();

        listener = new AmqpSource.EventBatchDeliveryListener(source, sourceCounter, SOURCE_ID, false, clock);
        Event event = listener.createEventFrom(delivery);

        assertThat(event.getHeaders(), hasEntry(Constants.Event.TIMESTAMP, String.valueOf(SYSTEM_DATE.getMillis())));
    }

    @Test
    public void testCreateEventFrom_MissingAppId() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getAppId()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.APP_ID)));
    }

    @Test
    public void testCreateEventFrom_MissingContentType() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getContentType()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.CONTENT_TYPE)));
    }

    @Test
    public void testCreateEventFrom_MissingContentEncoding() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getContentEncoding()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.CONTENT_ENCODING)));
    }

    @Test
    public void testCreateEventFrom_MissingCorrelationId() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getCorrelationId()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.CORRELATION_ID)));
    }

    @Test
    public void testCreateEventFrom_MissingMessageId() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getMessageId()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.MESSAGE_ID)));
    }

    @Test
    public void testCreateEventFrom_MissingExpiration() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getExpiration()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.EXPIRATION)));
    }

    @Test
    public void testCreateEventFrom_MissingType() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getType()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.TYPE)));
    }

    @Test
    public void testCreateEventFrom_MissingUserId() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getUserId()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(Constants.Event.USER_ID)));
    }

    @Test
    public void testCreateEventFrom_MissingHeaders() throws Exception {
        QueueingConsumer.Delivery delivery = createDelivery();
        when(delivery.getProperties().getHeaders()).thenReturn(null);

        Event event = listener.createEventFrom(delivery);
        assertThat(event.getHeaders(), not(hasKey(KEY_1)));
        assertThat(event.getHeaders(), not(hasKey(KEY_2)));
    }

    static List listOfSize(int size) {
        return argThat(new IsListOfSize(size));
    }

    static class IsListOfSize extends ArgumentMatcher<List> {
        private final int size;

        IsListOfSize(int size) {
            this.size = size;
        }

        public boolean matches(Object list) {
            return ((List) list).size() == size;
        }
    }
}