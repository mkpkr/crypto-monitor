package com.mike.cryptomonitor.serde;

import org.apache.kafka.common.serialization.Serdes.WrapperSerde;

import com.mike.cryptomonitor.model.PriceTick;

/**
 * @deprecated use org.springframework.kafka.support.serializer.JsonSerde
 */
@Deprecated
public class PriceTickSerde extends WrapperSerde<PriceTick> {

	public PriceTickSerde() {
		super(new PriceTickSerializer(), new PriceTickDeserializer());
	}
	
}
