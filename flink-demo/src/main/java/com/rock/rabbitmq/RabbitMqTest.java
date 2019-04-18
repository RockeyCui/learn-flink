package com.rock.rabbitmq;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * @author RockeyCui
 */
public class RabbitMqTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("localhost")
                .setPort(5672)
                .setVirtualHost("/")
                .setUserName("rock")
                .setPassword("123456")
                .build();*/
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setUri("amqp://rock:123456@localhost:5672/%2f")
                .build();


        final DataStream<String> stream = env
                .addSource(new RMQSource<>(
                        connectionConfig,
                        "rockroll",
                        true,
                        new SimpleStringSchema()))
                .setParallelism(1);

        stream.print();

        env.execute("Flink add rabbitMQ data source");

    }
}
