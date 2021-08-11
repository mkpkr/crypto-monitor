package com.mike.cryptomonitor.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.DeserializationException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mike.cryptomonitor.model.PriceTick;

/**
 * @deprecated use org.springframework.kafka.support.serializer.JsonDeserializer
 */
@Deprecated
public class PriceTickDeserializer implements Deserializer<PriceTick> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public PriceTick deserialize(String topic, byte[] bytes) {
        if (bytes == null)
            return null;

        PriceTick data;
        try {
            data = objectMapper.readValue(bytes, PriceTick.class);
        } catch (Exception e) {
            throw new DeserializationException("Error deserializing object", bytes, false, e);
        }

        return data;
    }

}
