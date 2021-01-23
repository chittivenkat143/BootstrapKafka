package com.kafka.handson1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    //String topicName = "users";
    String topicName = "users-replicated";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> properties) {
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    public static Map<String, Object> kafkaProducerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090, localhost:9091, localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public void close(){
        kafkaProducer.close();
    }

    public void publishMessagesAsync(String key, String val){
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, val);
        kafkaProducer.send(producerRecord, getCallback(key, val));
    }

    private Callback getCallback(String key, String val) {
        return (metadata, exception) -> {
            if (exception != null) {
                logger.error("Exception in publishMessageSync: {}", exception.getMessage());
            } else {
                logger.info("Message {} sent successfully for the key {}", val, key);
                logger.info("Published Message Offset is {} and the Partition is {}", metadata.partition(), metadata.offset());
            }
        };
    }

    public void publishMessageSync(String key, String val) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, val);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = kafkaProducer.send(producerRecord).get();
            //System.out.println("Partition: " +recordMetadata.partition()+ " # Offset: " +recordMetadata.offset() );
            logger.info("Message {} sent successfully for the key {}", val, key);
            logger.info("Published Message Offset is {} and the Partition is {}", recordMetadata.partition(), recordMetadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("Exception in publishMessageSync: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(kafkaProducerProperties());
        messageProducer.publishMessageSync("B", "NewValues1");
        messageProducer.publishMessageSync("A", "NewValues2");
        /*messageProducer.publishMessagesAsync(null, "NewAsyncValues");
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("Exception in Main: {}", e.getMessage());
        }*/
    }
}
