package com.mike.cryptomonitor.serde;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.DeserializationException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mike.cryptomonitor.model.PriceTick;

public class PriceTickSerde extends WrapperSerde<PriceTick> {

	public PriceTickSerde() {
		super(new PriceTickSerializer(), new PriceTickDeserializer());
	}
	
}
