package com.mike.cryptomonitor;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.kafka.streams.kstream.Windowed;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import com.mike.cryptomonitor.model.CurrencyPair;
import com.mike.cryptomonitor.model.PriceTick;
import com.mike.cryptomonitor.service.PriceDifferenceProcessor;

import lombok.extern.slf4j.Slf4j;

@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration",
                classes = KafkaTestBase.KafkaConfiguration.class)
public class CryptoMonitorApplicationTests extends KafkaTestBase<PriceTick> {

	@Value("${cryptomonitor.input.topic}")
	String topic;
	
	@Value("${cryptomonitor.streaming.window.size.seconds}")
	int windowSizeSeconds;
	
	@Value("${cryptomonitor.streaming.window.advance.seconds}")
	int windowAdvanceSeconds;
	
	@Autowired PriceDifferenceProcessor priceDifferenceProcessor;

    @Test
    public void increase_by_1_every_2_seconds_gives_difference_of_5_per_10_seconds() throws Exception {
    	/* arrange */
    	String currency = "btc_usd";
    	String startDateTime = "2020-01-01T00:00:00.00Z";
    	
    	long eventTime = Instant.parse(startDateTime).toEpochMilli();
    	long timeIncrement = 2000L;
    	
    	BigDecimal price = BigDecimal.TEN;
    	BigDecimal priceIncrement = BigDecimal.ONE;
    	
    	/* act */
    	for(int i = 0; i < 50; i++) {
    		eventTime = eventTime + timeIncrement;
    		price = price.add(priceIncrement);
    		kafkaTemplate.send(topic, currency, new PriceTick("id"+i, eventTime, CurrencyPair.BTC_USD, price)).get();
   
    	}
    	
    	SortedMap<Windowed<String>, BigDecimal> priceDifferences = priceDifferenceProcessor.getPriceDifferences();

    	long finalEventTime = eventTime;
    	await().until(() -> priceDifferences.lastKey().window().start() == finalEventTime);

    	/* assert */
    	int boundary = windowSizeSeconds/windowAdvanceSeconds;
    	int idx = 0;

    	for(Entry<Windowed<String>, BigDecimal> entry : priceDifferences.entrySet()) {
    		//ignore the first iteration of windows - see doc on PriceTickProcessor for why
    		if(idx++ < boundary) {
    			continue;
    		}
    		//ignore the final iteration of windows as they will not represent real life scenario (each of the final windows will have fewer price ticks than the last as the process winds down)
    		if(idx >= priceDifferences.entrySet().size() - boundary) {
    			break;
    		}
    		
    		assertEquals(BigDecimal.valueOf(5), entry.getValue(), 
    				     () -> "Value for " + Instant.ofEpochMilli(entry.getKey().window().start()) + " is " + entry.getValue());
    	}
    	
    }

}
