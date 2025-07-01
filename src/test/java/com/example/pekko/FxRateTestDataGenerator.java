package com.example.pekko;

import com.example.pekko.model.FxRate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public class FxRateTestDataGenerator {
    
    private static final String[] CURRENCIES = {
        "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK"
    };
    
    private static final String[] SOURCES = {
        "Bloomberg", "Reuters", "XE", "OANDA", "CurrencyLayer", "Fixer"
    };
    
    private final Random random;
    
    public FxRateTestDataGenerator() {
        this.random = new Random(42); // Fixed seed for reproducible tests
    }
    
    public FxRateTestDataGenerator(long seed) {
        this.random = new Random(seed);
    }
    
    public List<FxRate> generateFxRates(int count) {
        List<FxRate> fxRates = new ArrayList<>();
        
        for (int i = 0; i < count; i++) {
            fxRates.add(generateSingleFxRate());
        }
        
        return fxRates;
    }
    
    public FxRate generateSingleFxRate() {
        String fromCurrency = CURRENCIES[random.nextInt(CURRENCIES.length)];
        String toCurrency;
        
        // Ensure from and to currencies are different
        do {
            toCurrency = CURRENCIES[random.nextInt(CURRENCIES.length)];
        } while (fromCurrency.equals(toCurrency));
        
        double baseRate = generateBaseRate(fromCurrency, toCurrency);
        double variance = (random.nextDouble() - 0.5) * 0.1; // ±5% variance
        double rate = baseRate * (1 + variance);
        
        FxRate fxRate = new FxRate();
        fxRate.setId(UUID.randomUUID().toString());
        fxRate.setFromCurrency(fromCurrency);
        fxRate.setToCurrency(toCurrency);
        fxRate.setRate(Math.round(rate * 10000.0) / 10000.0); // Round to 4 decimal places
        fxRate.setTimestamp(Instant.now().toEpochMilli());
        
        // 80% chance of having a source
        if (random.nextDouble() < 0.8) {
            fxRate.setSource(SOURCES[random.nextInt(SOURCES.length)]);
        }
        
        return fxRate;
    }
    
    private double generateBaseRate(String from, String to) {
        // Simplified base rates for common currency pairs
        // In reality, these would come from actual market data
        
        if ("USD".equals(from)) {
            switch (to) {
                case "EUR": return 0.85;
                case "GBP": return 0.73;
                case "JPY": return 110.0;
                case "CHF": return 0.92;
                case "CAD": return 1.25;
                case "AUD": return 1.35;
                default: return 1.0;
            }
        } else if ("EUR".equals(from)) {
            switch (to) {
                case "USD": return 1.18;
                case "GBP": return 0.86;
                case "JPY": return 129.0;
                default: return 1.0;
            }
        } else if ("GBP".equals(from)) {
            switch (to) {
                case "USD": return 1.37;
                case "EUR": return 1.16;
                case "JPY": return 151.0;
                default: return 1.0;
            }
        }
        
        // Default rate with some randomness
        return 0.5 + random.nextDouble() * 2.0;
    }
    
    public FxRate generateSpecificFxRate(String fromCurrency, String toCurrency, double rate) {
        FxRate fxRate = new FxRate();
        fxRate.setId(UUID.randomUUID().toString());
        fxRate.setFromCurrency(fromCurrency);
        fxRate.setToCurrency(toCurrency);
        fxRate.setRate(rate);
        fxRate.setTimestamp(Instant.now().toEpochMilli());
        fxRate.setSource("TestSource");
        return fxRate;
    }
}