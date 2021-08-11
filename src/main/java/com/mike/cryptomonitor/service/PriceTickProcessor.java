package com.mike.cryptomonitor.service;

import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Service;

import com.mike.cryptomonitor.model.PriceTick;

@Service("processor")
public class PriceTickProcessor implements Consumer<KStream<String, PriceTick>> {

	@Override
	public void accept(KStream<String, PriceTick> input) {

		input.foreach((k,v) -> System.out.println("##########key: " + k + "; value: " + v.getId()));
		
	}

}
