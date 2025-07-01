package com.example.pekko;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import com.example.pekko.model.FxRate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.time.Duration;
import org.apache.pekko.util.Timeout;

public class FxRateStreamProcessor {
    private static final Logger log = LoggerFactory.getLogger(FxRateStreamProcessor.class);
    
    private final ActorSystem<?> system;
    private final ConsumerSettings<String, Object> consumerSettings;
    private final RedisPublisher redisPublisher;
    private final ActorRef<FxRateStorage.Command> fxRateStorage;
    
    public FxRateStreamProcessor(ActorSystem<?> system, ActorRef<FxRateStorage.Command> fxRateStorage) {
        this.system = system;
        this.fxRateStorage = fxRateStorage;
        this.redisPublisher = new RedisPublisher();
        this.consumerSettings = createConsumerSettings();
    }
    
    private ConsumerSettings<String, Object> createConsumerSettings() {
        Map<String, Object> avroConfig = new HashMap<>();
        avroConfig.put("schema.registry.url", "http://localhost:8081");
        avroConfig.put("specific.avro.reader", true);

        KafkaAvroDeserializer valueDeserializer = new KafkaAvroDeserializer();
        valueDeserializer.configure(avroConfig, false); // false = not for keys

        return ConsumerSettings.create(system, new StringDeserializer(), valueDeserializer)
                .withBootstrapServers("localhost:9092")
                .withGroupId("fx-rate-processor")
                .withProperty("schema.registry.url", "http://localhost:8081")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    }
    
    public CompletionStage<Done> startProcessing() {
        log.info("Starting FxRate stream processing from Kafka topic 'fx-rates'");
        
        return Consumer.plainSource(consumerSettings, Subscriptions.topics("fx-rates"))
                .map(record -> {
                    FxRate fxRate = (FxRate) record.value();
                    log.debug("Received FxRate: {} {} -> {} at rate {}", 
                             fxRate.getId(), fxRate.getFromCurrency(), fxRate.getToCurrency(), fxRate.getRate());
                    return fxRate;
                })
                .mapAsync(10, fxRate -> {
                    // Store in ORMultiMap
                    Duration timeout = Duration.ofSeconds(5);
                    CompletionStage<FxRateStorage.StoreResponse> storeFuture = org.apache.pekko.actor.typed.javadsl.AskPattern.ask(
                            fxRateStorage,
                            replyTo -> new FxRateStorage.StoreFxRate(fxRate, replyTo),
                            timeout,
                            system.scheduler()
                    );
                    
                    // Also publish to Redis for backward compatibility
                    CompletionStage<FxRate> redisFuture = redisPublisher.publishFxRate(fxRate);
                    
                    // Combine both operations and return the FxRate
                    return storeFuture.thenCombine(redisFuture, (storeResult, redisResult) -> {
                        log.debug("Stored FxRate in ORMultiMap and Redis: {}", fxRate.getId());
                        return fxRate;
                    }).exceptionally(throwable -> {
                        log.error("Failed to store FxRate: {}", fxRate.getId(), throwable);
                        return fxRate; // Continue processing even if storage fails
                    });
                })
                .runWith(Sink.ignore(), system);
    }
}