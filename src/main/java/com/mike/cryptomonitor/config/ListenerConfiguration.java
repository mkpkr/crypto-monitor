package com.mike.cryptomonitor.config;

import java.util.function.Consumer;

import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ListenerConfiguration {

//    @Bean
//    public Consumer<KStream<String, String>> processor() {
//
//        return input -> System.out.println("###" + input);
//
//    }
}