package com.rock.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class Test {
    private KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;

    public static void main(String[] args) {
        //KafkaConsumer<String, String> kafkaConsumer = get("172.16.12.147:9094", "cirrostream_test_phone_filter_window_sink", "test1");
        // KafkaConsumer<String, String> kafkaConsumer = get("172.16.12.147:9094", "cirrostream_test_phone_filter_topic", "cuishilei");
        KafkaConsumer<String, String> kafkaConsumer = get("172.16.12.147:9094,172.16.12.148:9094,172.16.12.149:9094", "topic-ck-source-32", "aaa");

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }
    }


    private static KafkaConsumer<String, String> get(String server, String topic, String groupId) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", server);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        return kafkaConsumer;
    }
}
