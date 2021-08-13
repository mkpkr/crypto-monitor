package com.mike.cryptomonitor;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestComponent;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.ContextStartedEvent;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Component;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.support.TestPropertySourceUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import com.mike.cryptomonitor.model.PriceTick;

@Testcontainers
public class KafkaTestBase<T> {

	@Container
	public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.1.1"))
	                                            .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "4");

	@Autowired
	protected KafkaTemplate<String, T> kafkaTemplate;
	
	@DynamicPropertySource
	public static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.kafka.streams.binder.brokers", kafka::getBootstrapServers);
    }
	
	@TestConfiguration
	public static class KafkaConfiguration {
		@Bean
		public KafkaAdmin admin() {
		    Map<String, Object> configs = new HashMap<>();
		    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
		    return new KafkaAdmin(configs);
		}

		@Bean
		public NewTopic inputTopic(@Value("${cryptomonitor.input.topic}") String topicName) {
		    return TopicBuilder.name(topicName)
		    		           .partitions(4)
		                       .build();
		}

		@Bean
		public ProducerFactory<String, PriceTick> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
			configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
			configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
			configProps.put(ProducerConfig.ACKS_CONFIG, "0");
			
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean
		public KafkaTemplate<String, PriceTick> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}

	}
	
	

}
