package com.kafka.handson3.deserilazer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.handson3.Customer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CustomerDeserializer implements Deserializer<Customer> {
    private static final Logger logger = LoggerFactory.getLogger(CustomerDeserializer.class);
    ObjectMapper mapper = new ObjectMapper();
    @Override
    public Customer deserialize(String topic, byte[] data) {
        logger.info("Inside Consumer Deserialize");
        try {
            return mapper.readValue(data, Customer.class);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("Exception Occurred during deserializing {}", e);
        }
        return null;
    }
}
