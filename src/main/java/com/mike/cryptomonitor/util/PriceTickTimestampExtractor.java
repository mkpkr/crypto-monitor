package com.mike.cryptomonitor.util;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.springframework.stereotype.Component;

import com.mike.cryptomonitor.model.PriceTick;

/**
 * Get the event time of the price tick for windowing purposes
 */
@Component
public class PriceTickTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long previousTimestamp) {
        PriceTick priceTick = (PriceTick) consumerRecord.value();

        return priceTick.getTimestampMs();
    }

}