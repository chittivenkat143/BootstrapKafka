package com.kafka.handson3;

import com.kafka.handson3.deserilazer.CustomerDeserializer;
import com.kafka.handson3.listeners.MessageConsumerCustomerRebalance;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.kafka.handson3.listeners.MessageConsumerCustomerRebalance.serializiedFilePath;

public class MessageConsumerCustomDomain {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerCustomDomain.class.getName());

    private KafkaConsumer<Integer, Customer> kafkaConsumer;
    private String strTopic = "customers";

    private Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();

    public MessageConsumerCustomDomain(Map<String, Object> prop) {
        kafkaConsumer = new KafkaConsumer<Integer, Customer>(prop);
    }

    private static Map<String, Object> consumerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090, localhost:9091, localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_customers_one");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    public void consumeMessages(){
        kafkaConsumer.subscribe(List.of(strTopic), new MessageConsumerCustomerRebalance(kafkaConsumer));
        try {
            while (true) { /*Consumer Poll Loop*/
                ConsumerRecords<Integer, Customer> consumerRecords = kafkaConsumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                consumerRecords.forEach((cRecords) -> {
                    logger.info("Consumer RecordKey: {}, RecordValue: {}, & RecordPartition: {}", cRecords.key(), cRecords.value(), cRecords.partition());
                    offsetAndMetadataMap.put(new TopicPartition(cRecords.topic(), cRecords.partition()), new OffsetAndMetadata(cRecords.offset()+1,null));
                });

                if(consumerRecords.count() > 0){
                    //kafkaConsumer.commitSync();//Application will wait upto the commit success
                    //kafkaConsumer.commitAsync();//Application will not wait, for commit there should be another thread will run

                    // commits the last record offset read by the poll invocation
                    /*kafkaConsumer.commitAsync(offsetAndMetadataMap, (offsets, exception) -> { *//*Async Commit With offset&Metadata Callback*//*
                        if(exception!=null){
                            logger.error("Exception in consumeMessages: " + exception.getMessage());
                        }else{
                            logger.info("Offset Committed!");
                        }
                    });*/
                    logger.info("OffsetsMap : {}", offsetAndMetadataMap);
                    writeOffsetMapToPath(offsetAndMetadataMap);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Exception in consumeMessages: " + e.getMessage());
        } finally {
            kafkaConsumer.close();
        }
    }

    private void writeOffsetMapToPath(Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap) {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;

        try {
            fos = new FileOutputStream(serializiedFilePath);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(offsetAndMetadataMap);
            logger.info("Offsets Written Successfully!");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Exception while writing the file: {}", e);
        } finally {
            try {
                if(fos!=null)fos.close();
                if(oos!=null)oos.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("Exception while closing the stream: {}", e);
            }
        }

    }

    public static void main(String[] args) {
        MessageConsumerCustomDomain consumer = new MessageConsumerCustomDomain(consumerProperties());
        consumer.consumeMessages();
    }

}
