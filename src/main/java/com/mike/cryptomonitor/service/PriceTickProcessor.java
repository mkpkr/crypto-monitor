package com.mike.cryptomonitor.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import com.mike.cryptomonitor.model.PriceTick;

import lombok.Data;

@Service("processor")
public class PriceTickProcessor implements Consumer<KStream<String, PriceTick>> {

	private int windowSizeSeconds;
	private int windowAdvanceSeconds;
	
	public PriceTickProcessor(@Value("${cryptomonitor.streaming.window.size.seconds}") int windowSizeSeconds, 
			                  @Value("${cryptomonitor.streaming.window.advance.seconds}") int windowAdvanceSeconds) {
		this.windowSizeSeconds = windowSizeSeconds;
		this.windowAdvanceSeconds = windowAdvanceSeconds;
	}

	@Override
	public void accept(KStream<String, PriceTick> input) {
		
		//TODO idempotent processing

		input.peek((k,v) -> System.out.println("# Input: Key: " + k + "; Value " + v))
		     .groupByKey() //key is already currency pair so no need to repartition
		     .windowedBy(TimeWindows.of(Duration.ofSeconds(windowSizeSeconds))
		     .advanceBy(Duration.ofSeconds(windowAdvanceSeconds)))
		     .<PriceDiff>aggregate(PriceDiff::new,  
		    		   (key, newValue, aggregate) -> {
		    			   BigDecimal newDifference = newValue.getPrice().subtract(aggregate.getPreviousValue());
		    			   System.out.println("[setting");
//		    			   aggregate.setDifference(aggregate.getDifference().add(newDifference));
		    			   aggregate.setPreviousValue(newValue.getPrice());
		    			   aggregate.addTimestamp(newValue.getTimestampMs());
		    			   aggregate.addPrice(newValue.getPrice());
		    			   System.out.println("returning new]");
		    			   System.out.println("--key: " + key + " --newValue-price: " + newValue.getPrice() +  " --newValue-timestamp: " + newValue.getTimestampMs() + " --newValue-id: " + newValue.getId() +   "--newDifference: " + newDifference + "--aggregate: " + aggregate);
		    		       return new PriceDiff(aggregate);
		               },
		    		   Materialized.<String, PriceDiff, WindowStore<Bytes, byte[]>>as("price-differences3")
			                       .withKeySerde(new StringSerde())
		    		   			   .withValueSerde(new JsonSerde<>(PriceDiff.class)))
		     .mapValues(priceDiff -> priceDiff.getDifference())
		     .toStream()
		     .peek((k,v) -> System.out.println("# peek: Key: " + k + "; Value " + v))
		     .foreach((k,v) -> System.out.println("### Output: Key: " + k + "; Value " + v));
		     
		System.out.println("##### DONE");
		
	}
	
//	@Data
	private static class PriceDiff {
		static int count = 0;
		
		private int id;
		private BigDecimal firstValue;
		private BigDecimal previousValue;
		private BigDecimal difference;
		
		private List<Long> timestamps;
		private List<BigDecimal> prices;
		
		public PriceDiff() {
			id = ++count;
			System.out.println("~~~new price diff " + count);
			timestamps = new ArrayList<>();
			prices = new ArrayList<>();
		}
		
		public PriceDiff(PriceDiff original) {
			System.out.println("~~~copy ctor " + original.id);
			this.id = original.id;
//			this.firstValue = original.firstValue;
			this.previousValue = original.previousValue;
			this.difference = original.difference;
			
			this.timestamps = new ArrayList<>(original.timestamps);
		    this.prices = new ArrayList<>(original.prices);
		}
		
		public void updateValue(BigDecimal value) {
			if(previousValue != null) {
				if(difference == null) {
					
				} else { 
					
				}
			}
			
			previousValue = value;
		}

		/*
		 * JSON Getters and Setters
		 */
		public BigDecimal getPreviousValue() {
			System.out.println("~~~previous value for " + id + " is " + previousValue);
			return previousValue;
		}

		public void setPreviousValue(BigDecimal previousValue) {
			System.out.println("~~~setting previous value for " + id + " to " + previousValue);
//			if(firstValue == null) {
//				System.out.println("~~~setting first value for " + id + " to " + previousValue);
//				this.firstValue = previousValue;
//			}
			this.previousValue = previousValue;
		}
		
		public int getId() {
			return id;
		}

		public void setId(int id) {
			System.out.println("~~~setting id for " + this.id + " to " + id);
			this.id = id;
		}

		public BigDecimal getDifference() {
			System.out.println("~~~difference for " + id + " is " + difference);
			return difference;
//			System.out.println("~~~difference for " + id + " is " + difference + " but firstValue is " + firstValue);
//			return firstValue == null ? BigDecimal.ZERO : difference.subtract(firstValue);
		}

		public void setDifference(BigDecimal difference) {
			System.out.println("~~~setting difference for " + id + " to " + difference + " (first value is " + firstValue);
			this.difference = difference;
		}
		
		public void addTimestamp(long timestamp) {
			this.timestamps.add(timestamp);
		}
		
		public void addPrice(BigDecimal price) {
			this.prices.add(price);
		}

		public List<Long> getTimestamps() {
			return timestamps;
		}

		public List<BigDecimal> getPrices() {
			return prices;
		}
		
		

		public BigDecimal getFirstValue() {
			return firstValue;
		}

		public void setFirstValue(BigDecimal firstValue) {
			System.out.println("~~~setting first value for " + id + " to " + firstValue);
			this.firstValue = firstValue;
		}

		@Override
		public String toString() {
			return "PriceDiff [firstValue=" + firstValue + ", previousValue=" + previousValue + ", difference="
					+ difference + ", id=" + id + ", timestamps=" + timestamps + ", prices=" + prices + "]";
		}

		

		
		
		
		
	}

}
