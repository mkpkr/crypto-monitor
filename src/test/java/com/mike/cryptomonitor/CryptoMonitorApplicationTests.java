package com.mike.cryptomonitor;

import java.math.BigDecimal;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.concurrent.ListenableFuture;

import com.mike.cryptomonitor.model.CurrencyPair;
import com.mike.cryptomonitor.model.PriceTick;

@SpringBootTest(properties = "spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration",
                classes = KafkaTestBase.KafkaConfiguration.class)
public class CryptoMonitorApplicationTests extends KafkaTestBase<PriceTick> {

	@Value("${cryptomonitor.input.topic}")
	String topic;
	
	@BeforeEach
	public void setUp() {
//		Map<String, Object> senderProps = producerProps(embeddedKafka);
//		senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
//        DefaultKafkaProducerFactory<String, PriceTick> pf = new DefaultKafkaProducerFactory<>(senderProps);
//        
//        template = new KafkaTemplate<>(pf, true);
//        template.setDefaultTopic("price-ticks");
	}

    @Test
    public void testAggregationOnPriceIncrease() throws Exception {
    	
    	long baseTime = 1628689754000L;
//    	long timeIncrement = 100L;
    	long timeIncrement = 2000L;
    	
    	BigDecimal basePrice = BigDecimal.TEN;
//    	BigDecimal priceIncrement = BigDecimal.valueOf(0.1);
    	BigDecimal priceIncrement = BigDecimal.ONE;
    	
    	for(int i = 0; i < 40; i++) {
    		long eventTime = baseTime + (i * timeIncrement);
    		
    		BigDecimal price = basePrice.add(BigDecimal.valueOf(i).multiply(priceIncrement));
    		
    		ListenableFuture<SendResult<String, PriceTick>> fut = kafkaTemplate.send(topic, "btc_usd", new PriceTick("id"+i, eventTime, CurrencyPair.BTC_USD, price));
    		SendResult<String, PriceTick> result = fut.get();
    		System.out.println(result.getProducerRecord().value());
    		Thread.sleep(100);
    	}
    	
//    	Thread.sleep(10000);

    }

}
