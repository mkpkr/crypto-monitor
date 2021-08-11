package com.mike.cryptomonitor.serde;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mike.cryptomonitor.model.PriceTick;

public class PriceTickSerializer implements Serializer<PriceTick> {
	
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, PriceTick data) {
        if (data == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

}
