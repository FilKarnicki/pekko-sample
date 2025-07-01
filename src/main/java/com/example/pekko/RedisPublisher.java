package com.example.pekko;

import com.example.pekko.model.FxRate;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class RedisPublisher {
    private static final Logger log = LoggerFactory.getLogger(RedisPublisher.class);
    
    private final RedisClient redisClient;
    private final GenericObjectPool<StatefulRedisConnection<String, String>> connectionPool;
    private final ObjectMapper objectMapper;
    
    public RedisPublisher() {
        this.objectMapper = new ObjectMapper();
        this.redisClient = createRedisClient();
        this.connectionPool = createConnectionPool();
    }
    
    private RedisClient createRedisClient() {
        RedisURI redisUri = RedisURI.Builder
                .redis("localhost", 6379)
                .build();
        return RedisClient.create(redisUri);
    }
    
    private GenericObjectPool<StatefulRedisConnection<String, String>> createConnectionPool() {
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig = 
                new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        
        return ConnectionPoolSupport.createGenericObjectPool(
                () -> redisClient.connect(), poolConfig);
    }
    
    public CompletionStage<FxRate> publishFxRate(FxRate fxRate) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                StatefulRedisConnection<String, String> connection = connectionPool.borrowObject();
                try {
                    RedisAsyncCommands<String, String> commands = connection.async();
                    
                    String key = String.format("fx_rate:%s_%s", fxRate.getFromCurrency(), fxRate.getToCurrency());
                    String jsonValue = convertToJson(fxRate);
                    
                    // Set the key-value pair with expiration
                    commands.setex(key, 3600, jsonValue); // Expire after 1 hour
                    
                    // Also publish to a channel for real-time subscribers
                    String channel = "fx_rate_updates";
                    commands.publish(channel, jsonValue);
                    
                    log.debug("Published FxRate to Redis: {} = {}", key, jsonValue);
                    return fxRate;
                    
                } finally {
                    connectionPool.returnObject(connection);
                }
            } catch (Exception e) {
                log.error("Failed to publish FxRate to Redis", e);
                throw new RuntimeException("Redis publish failed", e);
            }
        });
    }
    
    private String convertToJson(FxRate fxRate) {
        try {
            ObjectNode json = objectMapper.createObjectNode();
            json.put("id", fxRate.getId().toString());
            json.put("fromCurrency", fxRate.getFromCurrency().toString());
            json.put("toCurrency", fxRate.getToCurrency().toString());
            json.put("rate", fxRate.getRate());
            json.put("timestamp", fxRate.getTimestamp());
            if (fxRate.getSource() != null) {
                json.put("source", fxRate.getSource().toString());
            }
            return objectMapper.writeValueAsString(json);
        } catch (Exception e) {
            log.error("Failed to convert FxRate to JSON", e);
            throw new RuntimeException("JSON conversion failed", e);
        }
    }
    
    public void close() {
        try {
            if (connectionPool != null) {
                connectionPool.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
            log.info("RedisPublisher closed successfully");
        } catch (Exception e) {
            log.error("Error closing RedisPublisher", e);
        }
    }
}