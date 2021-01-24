package com.kafka.handson3;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class MessageConsumerRebalance implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerRebalance.class.getName());
    KafkaConsumer<String, String> kafkaConsumer;
    public MessageConsumerRebalance(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsRevoked: {}", partitions);
        kafkaConsumer.commitSync();
        logger.info("onPartitionsRevoked:Offsets Committed for {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsAssigned: {}", partitions);
        //kafkaConsumer.seekToBeginning(partitions);//This will read all records from starting on every brought Up [Will use this Kafka DataStore]
        kafkaConsumer.seekToEnd(partitions);//This will read new records on every brought Up & ignores old records
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsLost: {}", partitions);
    }

}
