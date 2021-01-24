package com.kafka.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.domain.Customer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerSerializer implements Serializer<Customer> {
    private static final Logger logger = LoggerFactory.getLogger(CustomerSerializer.class);
    private ObjectMapper mapper = new ObjectMapper();
    @Override
    public byte[] serialize(String topic, Customer data) {
        logger.info("Inside serialization logic");
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            logger.error("Unable to serialize {}", data, e);
        }
        return new byte[0];
    }
}
