package com.example.pekko;

import com.example.pekko.grpc.FxRateMessage;
import com.example.pekko.model.FxRate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.Done;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.ddata.Key;
import org.apache.pekko.cluster.ddata.LWWMap;
import org.apache.pekko.cluster.ddata.LWWMapKey;
import org.apache.pekko.cluster.ddata.LWWRegister;
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

import static com.example.pekko.FxRateGrpcServer.StreamSubscriberActor.convertToGrpcMessage;

public class FxRateStreamProcessor extends AbstractBehavior<FxRateStreamProcessor.Command> {
    private static final Logger log = LoggerFactory.getLogger(FxRateStreamProcessor.class);
    public static final Key<LWWMap<String, FxRateMessage>> FX_RATES_KEY = LWWMapKey.<String, FxRateMessage>create("fx-rates");
    
    public interface Command {}
    
    public static final class StartProcessing implements Command {
        public final ActorRef<Done> replyTo;
        
        public StartProcessing(ActorRef<Done> replyTo) {
            this.replyTo = replyTo;
        }
    }
    
    public static final class StoreFxRate implements Command {
        public final FxRate fxRate;
        
        public StoreFxRate(FxRate fxRate) {
            this.fxRate = fxRate;
        }
    }
    
    
    private final ActorSystem<?> system;
    private final ConsumerSettings<String, Object> consumerSettings;
    private final RedisPublisher redisPublisher;
    private final ActorRef<Replicator.Command> replicator;
    private final SelfUniqueAddress node;
    private final FxRateTimestampExtractor timestampExtractor;
    
    public static Behavior<Command> create(ActorSystem<?> system, RedisPublisher redisPublisher) {
        return Behaviors.setup(context -> new FxRateStreamProcessor(context, system, redisPublisher));
    }
    
    private FxRateStreamProcessor(ActorContext<Command> context, ActorSystem<?> system, RedisPublisher redisPublisher) {
        super(context);
        this.system = system;
        this.redisPublisher = redisPublisher;
        this.replicator = DistributedData.get(system).replicator();
        this.node = DistributedData.get(system).selfUniqueAddress();
        this.timestampExtractor = FxRateTimestampExtractor.getConfigured();
        this.consumerSettings = createConsumerSettings();
        
        log.info("Using timestamp extractor: {}", timestampExtractor);
    }
    
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartProcessing.class, this::onStartProcessing)
                .onMessage(StoreFxRate.class, this::onStoreFxRate)
                .build();
    }
    
    private Behavior<Command> onStartProcessing(StartProcessing command) {
        log.info("Starting FxRate stream processing from Kafka topic 'fx-rates'");
        final var committerSettings = CommitterSettings.create(system);
        
        Consumer.committableSource(consumerSettings, Subscriptions.topics("fx-rates"))
                .map(record -> {
                    FxRate fxRate = (FxRate) record.record().value();
                    log.debug("Received FxRate: {} {} -> {} at rate {}", 
                             fxRate.getId(), fxRate.getFromCurrency(), fxRate.getToCurrency(), fxRate.getRate());
                    return Pair.create(fxRate, record.committableOffset());
                })
                .mapAsync(10, pair -> {
                    // Send to self for processing
                    getContext().getSelf().tell(new StoreFxRate(pair.first()));
                    
                    // Also publish to Redis for backward compatibility
                    return redisPublisher.publishFxRate(pair.first())
                            .thenApply(redisResult -> {
                                log.debug("Stored FxRate in Redis: {}", pair.first().getId());
                                return pair.second();
                            }).exceptionally(throwable -> {
                                log.error("Failed to store FxRate in Redis: {}", pair.first().getId(), throwable);
                                return pair.second(); // Continue processing even if Redis fails
                            });
                }).toMat(Committer.sink(committerSettings), Keep.right())
                .run(system)
                .whenComplete((done, throwable) -> {
                    if (throwable != null) {
                        log.error("Stream processing failed", throwable);
                    } else {
                        log.info("Stream processing completed successfully");
                    }
                    command.replyTo.tell(done);
                });
        
        return this;
    }
    
    private Behavior<Command> onStoreFxRate(StoreFxRate command) {
        try {
            FxRate fxRate = command.fxRate;
            final FxRateMessage rateMsg = convertToGrpcMessage(fxRate, "Kafka");
            String currencyPair = fxRate.getFromCurrency() + "_" + fxRate.getToCurrency();
            long timestamp = timestampExtractor.apply(fxRate);

            replicator.tell(new Replicator.Update<>(
                    FX_RATES_KEY,
                    LWWMap.empty(),
                    Replicator.writeLocal(),
                    system.ignoreRef(), // Fire and forget - no response needed
                    curr -> curr.put(node, currencyPair, rateMsg, new LWWRegister.Clock<FxRateMessage>() {
                        @Override
                        public long apply(long currentTimestamp, FxRateMessage value) {
                            return timestamp; // Use our configurable timestamp
                        }
                    })));

            log.debug("Sent LWWMap update for FxRate: {} = {} (timestamp: {})", currencyPair, fxRate.getId(), timestamp);
        } catch (Exception e) {
            log.error("Failed to store FxRate in LWWMap: {}", command.fxRate.getId(), e);
        }
        
        return this;
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
}