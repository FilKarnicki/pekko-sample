package com.example.pekko;

import com.example.pekko.model.FxRate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.cluster.ddata.Key;
import org.apache.pekko.cluster.ddata.ORMultiMap;
import org.apache.pekko.cluster.ddata.ORMultiMapKey;
import org.apache.pekko.cluster.ddata.SelfUniqueAddress;
import org.apache.pekko.cluster.ddata.typed.javadsl.DistributedData;
import org.apache.pekko.cluster.ddata.typed.javadsl.Replicator;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.kafka.CommitterSettings;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Committer;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.javadsl.Keep;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class FxRateStreamProcessor {
    private static final Logger log = LoggerFactory.getLogger(FxRateStreamProcessor.class);
    public static final Key<ORMultiMap<String, String>> FX_RATES_KEY = ORMultiMapKey.<String, String>create("fx-rates");
    
    private final ActorSystem<?> system;
    private final ConsumerSettings<String, Object> consumerSettings;
    private final RedisPublisher redisPublisher;
    private final ActorRef<Replicator.Command> replicator;
    private final SelfUniqueAddress node;
    private final ObjectMapper objectMapper;
    
    public FxRateStreamProcessor(ActorSystem<?> system, RedisPublisher redisPublisher) {
        this.system = system;
        this.redisPublisher = redisPublisher;
        this.replicator = DistributedData.get(system).replicator();
        this.node = DistributedData.get(system).selfUniqueAddress();
        this.objectMapper = new ObjectMapper();
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
                .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    }
    
    public CompletionStage<Done> startProcessing() {
        log.info("Starting FxRate stream processing from Kafka topic 'fx-rates'");
        final var committerSettings = CommitterSettings.create(system);
        return Consumer.committableSource(consumerSettings, Subscriptions.topics("fx-rates"))
                .map(record -> {
                    FxRate fxRate = (FxRate) record.record().value();
                    log.debug("Received FxRate: {} {} -> {} at rate {}", 
                             fxRate.getId(), fxRate.getFromCurrency(), fxRate.getToCurrency(), fxRate.getRate());
                    return Pair.create(fxRate, record.committableOffset());
                })
                .mapAsync(10, pair -> {
                    // Store in ORMultiMap directly
                    final var fxRate = pair.first();
                    try {
                        String currencyPair = fxRate.getFromCurrency() + "_" + fxRate.getToCurrency();
                        String fxRateJson = objectMapper.writeValueAsString(convertFxRateToJson(fxRate));

                        replicator.tell(new Replicator.Update<>(
                                FX_RATES_KEY,
                                ORMultiMap.emptyWithValueDeltas(),
                                Replicator.writeLocal(),
                                null,
                                curr -> curr.addBinding(node, currencyPair, fxRateJson)));

                        log.debug("Stored FxRate in ORMultiMap: {} = {}", currencyPair, fxRateJson);
                    } catch (Exception e) {
                        log.error("Failed to store FxRate in ORMultiMap: {}", fxRate.getId(), e);
                    }
                    
                    // Also publish to Redis for backward compatibility
                    return redisPublisher.publishFxRate(fxRate)
                            .thenApply(redisResult -> {
                                log.debug("Stored FxRate in ORMultiMap and Redis: {}", fxRate.getId());
                                return pair.second();
                            }).exceptionally(throwable -> {
                                log.error("Failed to store FxRate in Redis: {}", fxRate.getId(), throwable);
                                return pair.second(); // Continue processing even if Redis fails
                            });
                }).toMat(Committer.sink(committerSettings), Keep.right())
                .run(system);
    }
    
    private Object convertFxRateToJson(FxRate fxRate) {
        return new Object() {
            public final String id = fxRate.getId().toString();
            public final String fromCurrency = fxRate.getFromCurrency().toString();
            public final String toCurrency = fxRate.getToCurrency().toString();
            public final double rate = fxRate.getRate();
            public final long timestamp = fxRate.getTimestamp();
            public final String source = fxRate.getSource() != null ? fxRate.getSource().toString() : null;
        };
    }
}