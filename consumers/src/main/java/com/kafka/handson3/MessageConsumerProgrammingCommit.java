package com.kafka.handson3;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MessageConsumerProgrammingCommit {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerProgrammingCommit.class.getName());

    private KafkaConsumer<String, String> kafkaConsumer;
    private String strTopic = "users-replicated";

    private Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();

    public MessageConsumerProgrammingCommit(Map<String, Object> prop) {
        kafkaConsumer = new KafkaConsumer<String, String>(prop);
    }

    private static Map<String, Object> consumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090, localhost:9091, localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_users_two");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    public void consumeMessages(){
        kafkaConsumer.subscribe(List.of(strTopic));
        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach((cRecords) -> {
                    logger.info("Consumer RecordKey: {}, RecordValue: {}, & RecordPartition: {}", cRecords.key(), cRecords.value(), cRecords.partition());
                    offsetAndMetadataMap.put(new TopicPartition(cRecords.topic(), cRecords.partition()), new OffsetAndMetadata(cRecords.offset()));
                });

                if(consumerRecords.count() > 0){
                    //kafkaConsumer.commitSync();//Application will wait upto the commit success
                    //kafkaConsumer.commitAsync();//Application will not wait, for commit there should be another thread will run
                    kafkaConsumer.commitAsync(offsetAndMetadataMap, (offsets, exception) -> { /*Async Commit With offset&Metadata Callback*/
                        if(exception!=null){
                            logger.error("Exception in consumeMessages: " + exception.getMessage());
                        }else{
                            logger.info("Offset Committed!");
                        }
                    });
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception in consumeMessages: " + e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    public static void main(String[] args) {
        MessageConsumerProgrammingCommit consumer = new MessageConsumerProgrammingCommit(consumerProperties());
        consumer.consumeMessages();
    }

}
