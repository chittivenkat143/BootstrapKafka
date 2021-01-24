package com.kafka.handson3;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumer {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class.getName());

    private KafkaConsumer<String, String> kafkaConsumer;
    private String strTopic = "users-replicated";

    public MessageConsumer(Map<String, Object> prop) {
        kafkaConsumer = new KafkaConsumer<String, String>(prop);
    }

    private static Map<String, Object> consumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090, localhost:9091, localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_users_one");
        return properties;
    }

    public void consumeMessages(){
        kafkaConsumer.subscribe(List.of(strTopic));
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach((cRecords) -> {
                    logger.info("Consumer RecordKey: {}, RecordValue: {}, & RecordPartition: {}", cRecords.key(), cRecords.value(), cRecords.partition());
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception in consumeMessages: " + e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumer consumer = new MessageConsumer(consumerProperties());
        consumer.consumeMessages();
    }

}
