package com.mike.cryptomonitor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.utils.KafkaTestUtils.consumerProps;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getRecords;
import static org.springframework.kafka.test.utils.KafkaTestUtils.producerProps;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.mike.cryptomonitor.model.CurrencyPair;
import com.mike.cryptomonitor.model.PriceTick;
import com.mike.cryptomonitor.serde.PriceTickSerializer;
import com.mike.cryptomonitor.service.PriceTickProcessor;

@DirtiesContext
@EmbeddedKafka(controlledShutdown=false, 
               partitions=4,
               topics={"price-ticks"})
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE,
                properties = {"spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration"})
public class CryptoMonitorApplicationTests {

	@Autowired
    EmbeddedKafkaBroker embeddedKafka;
	
//	@Autowired
//	PriceTickProcessor processor;
	
	KafkaTemplate<String, PriceTick> template;
//	Consumer<String, String> consumer;
	
	@BeforeEach
	public void setUp() {
		Map<String, Object> senderProps = producerProps(embeddedKafka);
		senderProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		senderProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PriceTickSerializer.class);
        DefaultKafkaProducerFactory<String, PriceTick> pf = new DefaultKafkaProducerFactory<>(senderProps);
        
        
//		Map<String, Object> consumerProps = consumerProps("group", "false", embeddedKafka);
//        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, String.class);
//        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, String.class);
        
        
//        ConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
//    	consumer = cf.createConsumer();
//    	embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "price-ticks");
    	
        template = new KafkaTemplate<>(pf, true);
        template.setDefaultTopic("price-ticks");
	}

    @Test
    public void simpleTest() throws Exception {
    	
    	for(int i = 0; i < 1000; i++) {
    		template.sendDefault("btc_usd", new PriceTick("id"+i, CurrencyPair.BTC_USD, BigDecimal.valueOf(1.23)));
    	}
    }

}
