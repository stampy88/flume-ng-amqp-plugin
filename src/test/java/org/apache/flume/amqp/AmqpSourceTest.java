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
import org.apache.flume.Context;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.junit.Assert.assertThat;

/**
 * @author dave sinclair(stampy88@yahoo.com)
 */
public class AmqpSourceTest extends BaseAmqpTest {

    private static final String BINDINGS_CTX_PARAM = BINDING_KEY_1 + " " + BINDING_KEY_2;
    private static final String HOST_NAME = "hostName";
    private static final int PORT = 5555;
    private static final String VIRTUAL_HOST = "virtualHost";
    private static final String USER_NAME = "userName";
    private static final String PASSWORD = "password";
    private static final int CONNECTION_TIMEOUT = 9999;
    private static final int REQUEST_HEARTBEAT = 11111;

    @Test
    public void testCreateConsumerBuilderFrom() throws Exception {
        Context ctx = createContext();

        AmqpConsumer.Builder builder = AmqpSource.createConsumerBuilderFrom(ctx);

        assertThat(builder.getExchangeName(), is(EXCHANGE_NAME));
        assertThat(builder.getExchangeType(), is(Constants.AMQP.DIRECT_EXCHANGE));
        assertThat(builder.isDurableExchange(), is(DURABLE_EXCHANGE));
        assertThat(builder.getQueueName(), is(QUEUE_NAME));
        assertThat(builder.isDurableQueue(), is(DURABLE_QUEUE));
        assertThat(builder.isExclusiveQueue(), is(EXCLUSIVE_QUEUE));
        assertThat(builder.isAutoDeleteQueue(), is(AUTO_DELETE_QUEUE));
        assertThat(builder.getBindings(), hasItemInArray(BINDING_KEY_1));
        assertThat(builder.getBindings(), hasItemInArray(BINDING_KEY_2));
        assertThat(builder.getPrefetchSize(), is(PREFETCH_SIZE));
        assertThat(builder.getBatchSize(), is(BATCH_SIZE));
        assertThat(builder.isAutoAck(), is(AUTO_ACK));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateConsumerBuilderFrom_MissingExchangeName() throws Exception {
        Context ctx = createContext();
        ctx.clear();

        AmqpSource.createConsumerBuilderFrom(ctx);
    }

    @Test
    public void testCreateConsumerBuilderFrom_NullBindings() throws Exception {
        Context ctx = new Context();
        ctx.put(AmqpSourceConfigurationConstants.EXCHANGE_NAME, EXCHANGE_NAME);

        AmqpConsumer.Builder builder = AmqpSource.createConsumerBuilderFrom(ctx);

        assertThat(builder.getBindings(), is(Constants.Defaults.BINDINGS));
    }

    @Test
    public void testCreateConsumerBuilderFrom_DefaultPrefetch() throws Exception {
        Context ctx = new Context();
        ctx.put(AmqpSourceConfigurationConstants.EXCHANGE_NAME, EXCHANGE_NAME);

        AmqpConsumer.Builder builder = AmqpSource.createConsumerBuilderFrom(ctx);

        assertThat(builder.getPrefetchSize(), is(Constants.Defaults.PREFETCH_SIZE));
    }

    @Test
    public void testCreateConnectionFactoryFrom() throws Exception {
        Context ctx = createContext();

        ConnectionFactory connectionFactory = AmqpSource.createConnectionFactoryFrom(ctx);

        assertThat(connectionFactory.getHost(), is(HOST_NAME));
        assertThat(connectionFactory.getPort(), is(PORT));
        assertThat(connectionFactory.getVirtualHost(), is(VIRTUAL_HOST));
        assertThat(connectionFactory.getUsername(), is(USER_NAME));
        assertThat(connectionFactory.getPassword(), is(PASSWORD));
        assertThat(connectionFactory.getConnectionTimeout(), is(CONNECTION_TIMEOUT));
        assertThat(connectionFactory.getRequestedHeartbeat(), is(REQUEST_HEARTBEAT));
    }

    private Context createContext() {
        Context ctx = new Context();

        // connection factory attrs
        ctx.put(AmqpSourceConfigurationConstants.HOST, HOST_NAME);
        ctx.put(AmqpSourceConfigurationConstants.PORT, String.valueOf(PORT));
        ctx.put(AmqpSourceConfigurationConstants.VIRTUAL_HOST, VIRTUAL_HOST);
        ctx.put(AmqpSourceConfigurationConstants.USER_NAME, USER_NAME);
        ctx.put(AmqpSourceConfigurationConstants.PASSWORD, PASSWORD);
        ctx.put(AmqpSourceConfigurationConstants.CONNECTION_TIMEOUT, String.valueOf(CONNECTION_TIMEOUT));
        ctx.put(AmqpSourceConfigurationConstants.REQUEST_HEARTBEAT, String.valueOf(REQUEST_HEARTBEAT));

        // consumer attrs
        ctx.put(AmqpSourceConfigurationConstants.EXCHANGE_NAME, EXCHANGE_NAME);
        ctx.put(AmqpSourceConfigurationConstants.EXCHANGE_TYPE, Constants.AMQP.DIRECT_EXCHANGE);
        ctx.put(AmqpSourceConfigurationConstants.DURABLE_EXCHANGE, String.valueOf(DURABLE_EXCHANGE));
        ctx.put(AmqpSourceConfigurationConstants.QUEUE_NAME, QUEUE_NAME);
        ctx.put(AmqpSourceConfigurationConstants.DURABLE_QUEUE, String.valueOf(DURABLE_QUEUE));
        ctx.put(AmqpSourceConfigurationConstants.EXCLUSIVE_QUEUE, String.valueOf(EXCLUSIVE_QUEUE));
        ctx.put(AmqpSourceConfigurationConstants.AUTO_DELETE_QUEUE, String.valueOf(AUTO_DELETE_QUEUE));
        ctx.put(AmqpSourceConfigurationConstants.AUTO_ACK, String.valueOf(AUTO_ACK));
        ctx.put(AmqpSourceConfigurationConstants.BINDINGS, BINDINGS_CTX_PARAM);
        ctx.put(AmqpSourceConfigurationConstants.PREFETCH_SIZE, String.valueOf(PREFETCH_SIZE));
        ctx.put(AmqpSourceConfigurationConstants.BATCH_SIZE, String.valueOf(BATCH_SIZE));

        return ctx;
    }
}
