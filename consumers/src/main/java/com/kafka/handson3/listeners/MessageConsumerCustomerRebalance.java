package com.kafka.handson3.listeners;

import com.kafka.handson3.Customer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MessageConsumerCustomerRebalance implements ConsumerRebalanceListener {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerCustomerRebalance.class.getName());
    KafkaConsumer<Integer, Customer> kafkaConsumer;
    public static final String serializiedFilePath = "consumers/src/main/resources/offset.ser";

    public MessageConsumerCustomerRebalance(KafkaConsumer<Integer, Customer> kafkaConsumer) {
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
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = readOffsetSerializationFile();
        logger.info("onPartitionsAssigned: {}", partitions);
        logger.info("OffsetInfo: {}", offsetAndMetadataMap);
        //kafkaConsumer.seekToBeginning(partitions);//This will read all records from starting on every brought Up [Will use this Kafka DataStore]
        //kafkaConsumer.seekToEnd(partitions);//This will read new records on every brought Up & ignores old records
        if(offsetAndMetadataMap.size() > 0){
            partitions.forEach(partition->{
                kafkaConsumer.seek(partition, offsetAndMetadataMap.get(partition));
            });
        }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        logger.info("onPartitionsLost: {}", partitions);
    }

    private static Map<TopicPartition, OffsetAndMetadata> readOffsetSerializationFile(){
        logger.info("Reading Offset File");
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = new HashMap<>();
        FileInputStream fis=null;
        BufferedInputStream bis=null;
        ObjectInputStream ois=null;
        try {
             fis = new FileInputStream(serializiedFilePath);
             bis = new BufferedInputStream(fis);
             ois = new ObjectInputStream(bis);
             offsetAndMetadataMap = (Map<TopicPartition, OffsetAndMetadata>) ois.readObject();
            logger.info("Offset Map read from the path is: {}", serializiedFilePath);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            logger.info("Exception Occurred while reading the file: {}", e.getMessage());
        } finally {
            try {
                if(fis!=null)fis.close();
                if(bis!=null)bis.close();
                if(ois!=null)ois.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.info("Exception Occurred while closing the Streams: {}", e.getMessage());
            }
        }
        return offsetAndMetadataMap;
    }

}
