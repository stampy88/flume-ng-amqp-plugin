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
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author dave sinclair(stampy88@yahoo.com)
 */
public abstract class BaseAmqpTest {

    protected static final boolean DURABLE_EXCHANGE = true;
    protected static final boolean AUTO_DELETE_QUEUE = false;
    protected static final boolean AUTO_ACK= true;
    protected static final boolean DURABLE_QUEUE = false;
    protected static final boolean EXCLUSIVE_QUEUE = true;
    protected static final String EXCHANGE_NAME = "exchangeName";
    protected static final String QUEUE_NAME = "queueName";
    protected static final String BINDING_KEY_1 = "1";
    protected static final String BINDING_KEY_2 = "2";
    protected static final String[] BINDINGS = new String[]{BINDING_KEY_1, BINDING_KEY_2};
    protected static final Date MESSAGE_DATE = new Date();
    protected static final String APP_ID = "appId";
    protected static final String CONTENT_TYPE = "contentType";
    protected static final String CONTENT_ENCODING = "contentEncoding";
    protected static final String CORRELATION_ID = "correlationId";
    protected static final String MESSAGE_ID = "messageId";
    protected static final String EXPIRATION = "expiration";
    protected static final String TYPE = "type";
    protected static final String USER_ID = "userId";
    protected static final byte[] BODY = new byte[]{1, 2, 3};
    protected static final String KEY_1 = "key-1";
    protected static final String KEY_2 = "key-2";
    protected static final String VALUE_1 = "value-1";
    protected static final String VALUE_2 = "value-2";
    protected static final long DELIVERY_TAG = 99L;
    protected static final int PREFETCH_SIZE = 50;
    protected static final int BATCH_SIZE = 25;
    protected static final String ROUTING_KEY = "routing-key";

    protected QueueingConsumer.Delivery createDelivery() {
        return createDeliveryWithTag(DELIVERY_TAG);
    }

    protected QueueingConsumer.Delivery createDeliveryWithTag(long deliveryTag) {
        QueueingConsumer.Delivery delivery = mock(QueueingConsumer.Delivery.class);
        when(delivery.getBody()).thenReturn(BODY);

        Map<String, Object> headers = new HashMap<String, Object>();
              headers.put(KEY_1, VALUE_1);
              headers.put(KEY_2, VALUE_2);

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().
                timestamp(MESSAGE_DATE).
                appId(APP_ID).
                contentType(CONTENT_TYPE).
                contentEncoding(CONTENT_ENCODING).
                correlationId(CORRELATION_ID).
                messageId(MESSAGE_ID).
                expiration(EXPIRATION).
                type(TYPE).
                userId(USER_ID).
                headers(headers).build();

        when(delivery.getProperties()).thenReturn(properties);
        Envelope envelope = mock(Envelope.class);
        when(envelope.getDeliveryTag()).thenReturn(deliveryTag);
        when(envelope.getRoutingKey()).thenReturn(ROUTING_KEY);
        when(delivery.getEnvelope()).thenReturn(envelope);

        return delivery;
    }
}
