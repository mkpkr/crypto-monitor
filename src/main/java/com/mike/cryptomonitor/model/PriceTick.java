package com.mike.cryptomonitor.model;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Value;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PriceTick {
	
	private String id;
	private long timestampMs;
	private CurrencyPair ccy;
	private BigDecimal price;
	
}
