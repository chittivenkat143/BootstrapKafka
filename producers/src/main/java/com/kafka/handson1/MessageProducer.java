package com.kafka.handson1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class MessageProducer {

    String topicName = "users";
    KafkaProducer<String, String> kafkaProducer;

    public MessageProducer(Map<String, Object> properties) {
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    public static Map<String, Object> kafkaProducerProperties(){
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9090, localhost:9091, localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public void publishMessageSync(String key, String val) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, val);
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = kafkaProducer.send(producerRecord).get();
            System.out.println("Partition: " +recordMetadata.partition()+ " # Offset: " +recordMetadata.offset() );
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        MessageProducer messageProducer = new MessageProducer(kafkaProducerProperties());
        messageProducer.publishMessageSync(null, "ABC");
    }
}
