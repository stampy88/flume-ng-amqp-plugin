flume-ng-amqp-plugin
====================

The flume-ng-amqp-plugin allows you to use an AMQP broker as a Flume Source.

Usage
-----

The AMQP Event Source will take messages from an AMQP broker and create Flume events from the message. The body of the
event will contain the bytes from the message. All [properties](http://www.rabbitmq.com/releases/rabbitmq-java-client/v3.2.3/rabbitmq-java-client-javadoc-3.2.3/com/rabbitmq/client/BasicProperties.html)
of the message, including headers, will be transferred as headers in the flume event. A timestamp header will be added to
the event based either on the message's timestamp, or the system time depending on the configuration. The routing key from
the AMQP message will be added as a routingKey header to the event. In addition a sourceId header will be added
to the event that will contain an integer representing which instance of the source this is within an agent.

This source supports batching of events before sending into a channel.

The only required configuration parameter is the exchangeName parameter. All others will be defaulted.

* `exchangeName` - **required** - this is the name of the AMQP exchange we are getting messages from.
* `host` - the host name or IP address of the broker. Defaults to **localhost** when not specified.
* `port` - the port on which the broker is accepting connections. Defaults to **5672** when not specified.
* `virtualHost` - the virtual host to use when connecting to the broker. Default to **"/"** when not specified.
* `userName` - the AMQP user name to use when connecting to the broker. Defaults to **"guest"** when not specified.
* `password` - the password to use when connecting to the broker. Defaults to **"guest"** when not specified.
* `connectionTimeout` - connection establishment timeout in milliseconds; zero for infinite. Defaults to **zero**.
* `requestedHeartbeat` - the initially requested heartbeat interval, in seconds; zero for none. Defaults to **zero**.
* `exchangeType` -  the type exchange. Valid types are direct, fanout, topic, and header. Defaults to **direct** when not specified.
* `durableExchange` - true if we are declaring a durable exchange (the exchange will survive a server restart). Defaults to **false**.
* `queueName` - if left unspecified, the server chooses a name and provides this to the client. Generally, when applications
share a message queue they agree on a message queue name beforehand, and when an application needs a message queue
for its own purposes, it lets the server provide a name.
* `durableQueue` - if true, the message queue remains present and active when the server restarts. It may lose transient
messages if the server restarts. Defaults to **false**.
* `exclusiveQueue` - if true, the queue belongs to the current connection only, and is deleted when the connection closes.
Defaults to **false**.
* `autoDeleteQueue` - true if we are declaring an autodelete queue (server will delete it when no longer in use). Defaults to **false**.
* `autoAck` - i this field is set the server does not expect acknowledgements for messages. That is, when a message is
delivered to the client the server assumes the delivery will succeed and immediately dequeues it. This functionality
may increase performance but at the cost of reliability. Messages can get lost if a client dies before they are delivered
to the application. Defaults to **false**.
* `prefetchSize` - the client can request that messages be sent in advance so that when the client finishes processing a message,
the following message is already held locally, rather than needing to be sent down the channel. Prefetching gives a
performance improvement. The server will send a message in advance if it is equal to or smaller in size than the available
prefetch size (and also falls into other prefetch limits). May be set to zero, meaning "no specific limit", although other
prefetch limits may still apply. The prefetch-size is ignored if the no-ack option is set. Defaults to **zero**.
* `batchSize` - This property has dual purposes. If the source is not in auto-ack mode, then this will be the number of messages
to buffer before sending an ack to the server. Regardless of the ack mode, this property controls the number of events
that are transferred to a Flume channel at a time. Defaults to **100**.
* `bindings` - comma separated list of strings that will be used to bind the queue to the exchange. This is not required for
certain exchange types.
* `useMessageTimestamp` - if true, the timestamp for the Flume event will be based on the timestamp from the AMQP message.
Otherwise the system clock will be used. Defaults to **false**.

Examples
--------

1. Broker is on same node as flume agent, direct exchange type and two bindings

    ```
    agent1.sources.amqpSource.type = org.apache.flume.amqp.AmqpSource
    agent1.sources.amqpSource.exchangeName = alertMessages
    agent1.sources.amqpSource.bindings = system application
    ```

2. Broker is on different machine, direct exchange type, named queue

    ```
    agent1.sources.amqpSource.type = org.apache.flume.amqp.AmqpSource
    agent1.sources.amqpSource.host =  10.23.224.25
    agent1.sources.amqpSource.exchangeName = stockTrades
    agent1.sources.amqpSource.bindings  = GOOG APPL TSLA
    agent1.sources.amqpSource.queueName  = stockQueue
    ```

Building
--------

The AMQP Source is built using maven. Execute the

`mvn clean package`

this will tar.gz that contains the compiled classes and all dependent libraries. Extract this tgz into the plugins.d directory
in your Flume installation and restart the agent.
