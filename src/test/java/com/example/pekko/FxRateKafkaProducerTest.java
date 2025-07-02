package com.example.pekko;

import com.example.pekko.model.FxRate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class FxRateKafkaProducerTest {
    private static final Logger log = LoggerFactory.getLogger(FxRateKafkaProducerTest.class);
    
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC_NAME = "fx-rates";
    
    private FxRateAvroProducer producer;
    private FxRateTestDataGenerator dataGenerator;
    
    @BeforeEach
    void setUp() {
        log.info("Setting up FxRateKafkaProducerTest");
        producer = new FxRateAvroProducer(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, TOPIC_NAME);
        dataGenerator = new FxRateTestDataGenerator();
    }
    
    @AfterEach
    void tearDown() {
        log.info("Tearing down FxRateKafkaProducerTest");
        if (producer != null) {
            producer.close();
        }
    }
    
    @Test
    void testSendSingleFxRate() throws Exception {
        log.info("Testing single FxRate send");
        
        // Generate a single test FxRate
        FxRate fxRate = dataGenerator.generateSpecificFxRate("JPY", "JPY", 0.8500);
        
        // Send the FxRate
        var future = producer.sendFxRate(fxRate);
        var metadata = future.get(5, TimeUnit.SECONDS);
        
        // Verify successful send
        assertNotNull(metadata);
        assertTrue(metadata.offset() >= 0);
        assertEquals(TOPIC_NAME, metadata.topic());
        
        log.info("Successfully sent single FxRate to partition {} offset {}", 
                metadata.partition(), metadata.offset());
    }
    
    @Test
    void testSend100FxRates() throws Exception {
        log.info("Testing bulk send of 100 FxRates");
        
        // Generate 100 test FxRates
        List<FxRate> fxRates = dataGenerator.generateFxRates(100);
        assertEquals(100, fxRates.size());
        
        // Verify all FxRates have required fields
        for (FxRate fxRate : fxRates) {
            assertNotNull(fxRate.getId());
            assertNotNull(fxRate.getFromCurrency());
            assertNotNull(fxRate.getToCurrency());
            assertTrue(fxRate.getRate() > 0);
            assertTrue(fxRate.getTimestamp() > 0);
            assertNotEquals(fxRate.getFromCurrency(), fxRate.getToCurrency());
        }
        
        // Send all FxRates
        long startTime = System.currentTimeMillis();
        CompletableFuture<Void> sendFuture = producer.sendFxRates(fxRates);
        
        // Wait for all sends to complete
        sendFuture.get(30, TimeUnit.SECONDS);
        
        // Ensure all messages are flushed
        producer.flush();
        
        long endTime = System.currentTimeMillis();
        log.info("Successfully sent 100 FxRates in {} ms", endTime - startTime);
    }
    
    @Test
    void testSendVariousCurrencyPairs() throws Exception {
        log.info("Testing various currency pairs");
        
        // Create specific currency pairs
        List<FxRate> fxRates = List.of(
            dataGenerator.generateSpecificFxRate("USD", "EUR", 0.8500),
            dataGenerator.generateSpecificFxRate("EUR", "USD", 1.1765),
            dataGenerator.generateSpecificFxRate("GBP", "USD", 1.3500),
            dataGenerator.generateSpecificFxRate("USD", "JPY", 110.25),
            dataGenerator.generateSpecificFxRate("EUR", "GBP", 0.8600),
            dataGenerator.generateSpecificFxRate("CHF", "USD", 1.0850),
            dataGenerator.generateSpecificFxRate("AUD", "USD", 0.7400),
            dataGenerator.generateSpecificFxRate("CAD", "USD", 0.8000),
            dataGenerator.generateSpecificFxRate("USD", "SEK", 8.5000),
            dataGenerator.generateSpecificFxRate("NOK", "USD", 0.1150)
        );
        
        // Send all currency pairs
        CompletableFuture<Void> sendFuture = producer.sendFxRates(fxRates);
        sendFuture.get(10, TimeUnit.SECONDS);
        
        producer.flush();
        
        log.info("Successfully sent {} different currency pairs", fxRates.size());
    }
    
    @Test
    void testHighVolumeSend() throws Exception {
        log.info("Testing high volume send of 1000 FxRates");
        
        // Generate 1000 test FxRates
        List<FxRate> fxRates = dataGenerator.generateFxRates(1000);
        assertEquals(1000, fxRates.size());
        
        // Send all FxRates
        long startTime = System.currentTimeMillis();
        CompletableFuture<Void> sendFuture = producer.sendFxRates(fxRates);
        
        // Wait for all sends to complete
        sendFuture.get(60, TimeUnit.SECONDS);
        
        // Ensure all messages are flushed
        producer.flush();
        
        long endTime = System.currentTimeMillis();
        double throughput = 1000.0 / ((endTime - startTime) / 1000.0);
        
        log.info("Successfully sent 1000 FxRates in {} ms (throughput: {:.2f} msgs/sec)", 
                endTime - startTime, throughput);
        
        // Verify reasonable throughput (should be much faster than 1 msg/sec)
        assertTrue(throughput > 10, "Throughput should be greater than 10 msgs/sec");
    }
    
    @Test
    void testFxRateValidation() {
        log.info("Testing FxRate data validation");
        
        // Generate test data
        List<FxRate> fxRates = dataGenerator.generateFxRates(50);
        
        // Validate each FxRate
        for (FxRate fxRate : fxRates) {
            // Check required fields
            assertNotNull(fxRate.getId(), "FxRate ID should not be null");
            assertFalse(fxRate.getId().toString().isEmpty(), "FxRate ID should not be empty");
            
            assertNotNull(fxRate.getFromCurrency(), "From currency should not be null");
            assertNotNull(fxRate.getToCurrency(), "To currency should not be null");
            assertNotEquals(fxRate.getFromCurrency(), fxRate.getToCurrency(), 
                           "From and to currencies should be different");
            
            assertTrue(fxRate.getRate() > 0, "Exchange rate should be positive");
            assertTrue(fxRate.getTimestamp() > 0, "Timestamp should be positive");
            
            // Check currency format (should be 3 characters)
            assertEquals(3, fxRate.getFromCurrency().toString().length(), 
                        "From currency should be 3 characters");
            assertEquals(3, fxRate.getToCurrency().toString().length(), 
                        "To currency should be 3 characters");
            
            log.debug("Validated FxRate: {} {} -> {} at rate {}", 
                     fxRate.getId(), fxRate.getFromCurrency(), fxRate.getToCurrency(), fxRate.getRate());
        }
        
        log.info("Successfully validated {} FxRates", fxRates.size());
    }
}