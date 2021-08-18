package com.mike.cryptomonitor.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PriceDifferenceProcessorTests {
	
	PriceDifferenceProcessor sut;
	
	@BeforeEach
	public void setUp() {
		sut = new PriceDifferenceProcessor();
	}
	
	@Test
	public void priceDifferencesAreStored() {
		Windowed<String> windowedKey1 = new Windowed<>("btc_usd", new TimeWindow(1L, 2L));
		Windowed<String> windowedKey2 = new Windowed<>("xrp_gbp", new TimeWindow(2L, 3L));
		
		BigDecimal priceDiff1 =  BigDecimal.valueOf(1.0);
		BigDecimal priceDiff2 =  BigDecimal.valueOf(2.0);
		
		sut.process(windowedKey1, priceDiff1);
		sut.process(windowedKey2, priceDiff2);
		
		assertEquals(priceDiff1, sut.getPriceDifferences().get(windowedKey1));
		assertEquals(priceDiff2, sut.getPriceDifferences().get(windowedKey2));
	}
	
	@Test
	public void windowedKeysAreInCorrectOrder() {
		
		BigDecimal price = BigDecimal.ONE; //not important for the test - we are focused on key order

		Window window1 = new TimeWindow(1L, 2L);
		Window window2 = new TimeWindow(2L, 3L);
		Window window3 = new TimeWindow(3L, 4L);
		
		Map<Integer, Windowed<String>> testKeys = new HashMap<>();
		
		String ccy1 = "btc_usd";
		testKeys.put(0, new Windowed<>(ccy1, window1));
		testKeys.put(1, new Windowed<>(ccy1, window2));
		testKeys.put(2, new Windowed<>(ccy1, window3));
		
		String ccy2 = "eth_eur";
		testKeys.put(3, new Windowed<>(ccy2, window1));
		testKeys.put(4, new Windowed<>(ccy2, window2));
		testKeys.put(5, new Windowed<>(ccy2, window3));
		
		String ccy3 = "xrp_gbp";
		testKeys.put(6, new Windowed<>(ccy3, window1));
		testKeys.put(7, new Windowed<>(ccy3, window2));
		testKeys.put(8, new Windowed<>(ccy3, window3));
		
		//process unordered
		for(Integer i : List.of(3,4,7,1,8,6,0,2,5)) {
			sut.process(testKeys.get(i), price);
		}
		
		//verify they are in correct order
		int idx = 0;
		for(Windowed<String> key : sut.getPriceDifferences().keySet()) {
			assertEquals(testKeys.get(idx), key);
			idx++;
		}

	}

}
