package com.kafka.domain;

import com.kafka.serializer.CustomerSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducerDomain {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerDomain.class);

    //String topicName = "users";
    //String topicName = "users-replicated";
    String topicName = "customers";
    KafkaProducer<Integer, Customer> kafkaProducer;

    public MessageProducerDomain(Map<String, Object> properties) {
        kafkaProducer = new KafkaProducer<Integer, Customer>(properties);
    }

    public static Map<String, Object> kafkaProducerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090, localhost:9091, localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "10");
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");
        return properties;
    }

    public void close(){
        kafkaProducer.close();
    }

    public void publishMessageSync(Customer customer) {
        ProducerRecord<Integer, Customer> producerRecord = new ProducerRecord<>(topicName, customer.getId(), customer);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("Message {} sent successfully for the key {}", customer, customer.getId());
            logger.info("Published Message Offset is {} and the Partition is {}", recordMetadata.offset(), recordMetadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            logger.error("Exception in publishMessageSync: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        MessageProducerDomain messageProducer = new MessageProducerDomain(kafkaProducerProperties());
        for (Customer customer : StaticCustomers.getListOfCustomers()) {
            messageProducer.publishMessageSync(customer);
        }
    }

}
