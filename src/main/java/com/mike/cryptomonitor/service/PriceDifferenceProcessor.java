package com.mike.cryptomonitor.service;

import static java.util.Comparator.comparing;

import java.math.BigDecimal;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.kafka.streams.kstream.Windowed;
import org.springframework.stereotype.Service;

@Service
public class PriceDifferenceProcessor {
	
	private SortedMap<Windowed<String>, BigDecimal> priceDifferences;
	
	public PriceDifferenceProcessor() {
		priceDifferences = new TreeMap<>(comparing((Windowed<String> w) -> w.key())
				                         .thenComparingLong(w -> w.window().start()));
	}

	public void process(Windowed<String> windowedKey, BigDecimal priceDifference) {
		priceDifferences.put(windowedKey, priceDifference);
	}
	
	public SortedMap<Windowed<String>, BigDecimal> getPriceDifferences() {
		return priceDifferences;
	}

}
