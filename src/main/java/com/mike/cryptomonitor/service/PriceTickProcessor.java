package com.mike.cryptomonitor.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Service;

import com.mike.cryptomonitor.model.PriceTick;

import lombok.extern.slf4j.Slf4j;

/**
 * Processes price ticks from a kafka topic where key is currency and value is {@link com.mike.cryptomonitor.model.PriceTick}
 * 
 * Aggregates prices by time windows:
 * 
 * window size (seconds): cryptomonitor.streaming.window.size.seconds
 * window advance (seconds): advanced by cryptomonitor.streaming.window.advance.seconds
 * 
 * For example, if window size == 10 and advance == 1 then prices would be aggregated like...
 * 
 * [10:00:00, 10:00:10)
 * [10:00:01, 10:00:11)
 * [10:00:02, 10:00:12)
 * [10:00:03, 10:00:13)
 * [10:00:04, 10:00:14)
 * ...
 * 
 * NOTE: price differences will be wrong until a complete window cycle has passed (i.e. until the window size has elapsed once). 
 * This is because when a timestamp is first processed, previous windows will be created and backfilled, e.g. if window size is 10 and advance is 1, when the timestamp 10:00:10 is processed, 10 windows will be created that this price tick would be part of from [10:00:01, 10:00:11) - [10:00:10, 10:00:20).
 * When the application first starts and those previous windows are created, the "current price" isn't available for them, so the difference will not be correct. Once the time window has elapsed, in this case 10 seconds have passed, then newly created windows will have the current price available, and the price difference during those windows can be correctly calculated.
 * 
 * This could be fixed by calling out to a service that has historical prices when a time window is created, to get the starting price for that window, but for now we will settle for "eventual accuracy".
 * 
 *		
		 * TODO: concern - values will be wrong until a complete cycle of windows has passed e.g. until window size has elapsed
		 * this is because when the first value is encountered, it will backfill previous windows and these won't have correct 
		 * live prices to gauge a difference from
		 * for future we could use the constructor to call out to a service that got the price at the start time of the window
		 * 		not sure how to do that as you'd need to be aware of the window you're in when you create it
		 *
 */
@Service("processor")
@Slf4j
public class PriceTickProcessor implements Consumer<KStream<String, PriceTick>> {

	private PriceDifferenceProcessor priceDifferenceProcessor;
	private final int windowSizeSeconds;
	private final int windowAdvanceSeconds;
	private final String stateStoreName;
	
	public PriceTickProcessor(PriceDifferenceProcessor priceDifferenceProcessor,
			                  @Value("${cryptomonitor.streaming.window.size.seconds}") int windowSizeSeconds, 
			                  @Value("${cryptomonitor.streaming.window.advance.seconds}") int windowAdvanceSeconds,
			                  @Value("${cryptomonitor.kafka.state-store.name}") String stateStoreName) {
		this.priceDifferenceProcessor = priceDifferenceProcessor;
		this.windowSizeSeconds = windowSizeSeconds;
		this.windowAdvanceSeconds = windowAdvanceSeconds;
		this.stateStoreName = stateStoreName;
	}

	@Override
	public void accept(KStream<String, PriceTick> input) {
		
		//TODO idempotent processing

		input.groupByKey() //key is already currency pair so no need to repartition
		     .windowedBy(TimeWindows.of(Duration.ofSeconds(windowSizeSeconds))
			     .advanceBy(Duration.ofSeconds(windowAdvanceSeconds)))
		     .<PriceDiff>aggregate(PriceDiff::new,  
		    		   (key, newValue, aggregate) -> {
		    			   aggregate.update(newValue);
		    			   return new PriceDiff(aggregate);
		               },
		    		   Materialized.<String, PriceDiff, WindowStore<Bytes, byte[]>>as(stateStoreName + Instant.now().toEpochMilli())
			                       .withKeySerde(new StringSerde())
		    		   			   .withValueSerde(new JsonSerde<>(PriceDiff.class)))
		     .mapValues(priceDiff -> priceDiff.getDifference())
		     .toStream()
		     .foreach((k,v) -> priceDifferenceProcessor.process(k,v));
		
	}

	static class PriceDiff {
		
		static BigDecimal latest;

		private String id;
		private BigDecimal previousValue;
		private BigDecimal difference;
		
		//TODO remove these debug variables
		static int count = 0;
		private BigDecimal previousValueSetInCtor;
		
		private List<Long> timestamps;
		private List<BigDecimal> prices;
		
		public PriceDiff() {
			id = "" + ++count;
			
			previousValue = latest; //may be null, set in update() if it is
			previousValueSetInCtor = latest;
			timestamps = new ArrayList<>();
			prices = new ArrayList<>();
			difference = BigDecimal.ZERO;
		}
		
		public PriceDiff(PriceDiff original) {
			this.id = original.id;
			this.previousValue = original.previousValue;
			this.previousValueSetInCtor = original.previousValueSetInCtor;
			this.difference = original.difference;
			this.timestamps = new ArrayList<>(original.timestamps);
		    this.prices = new ArrayList<>(original.prices);
		}
		

		public void update(PriceTick priceTick) {
			
			if(previousValue == null) { //only called on startup for first window when latest was null
				previousValue = priceTick.getPrice();
			} else {
				BigDecimal newDifference = priceTick.getPrice().subtract(previousValue);
				difference = difference.add(newDifference);
			}
			
			if(id != priceTick.getId()) {
				id = priceTick.getId();
				latest = previousValue;
			}

			previousValue = priceTick.getPrice();
			
			
			
			addTimestamp(priceTick.getTimestampMs());
			addPrice(priceTick.getPrice());
		}

		/*
		 * JSON Getters and Setters
		 */
		public BigDecimal getPreviousValue() {
			return previousValue;
		}

		public void setPreviousValue(BigDecimal previousValue) {
			this.previousValue = previousValue;
		}
		
		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public BigDecimal getDifference() {
			return difference;
		}

		public void setDifference(BigDecimal difference) {
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

		public BigDecimal getPreviousValueSetInCtor() {
			return previousValueSetInCtor;
		}

		public void setPreviousValueSetInCtor(BigDecimal previousValueSetInCtor) {
			this.previousValueSetInCtor = previousValueSetInCtor;
		}

		@Override
		public String toString() {
			return "PriceDiff [previousValue=" + previousValue + ", difference=" + difference
					+ ", previousValueSetInCtor=" + previousValueSetInCtor + ", id=" + id + ", timestamps=" + timestamps
					+ ", prices=" + prices + "]";
		}


		
		
	}
	


}
