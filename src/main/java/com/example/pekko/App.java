package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.Done;
import org.apache.pekko.cluster.ddata.typed.javadsl.DistributedData;
import org.apache.pekko.cluster.ddata.typed.javadsl.Replicator;
import org.apache.pekko.cluster.ddata.LWWMap;
import org.apache.pekko.cluster.ddata.Key;
import org.apache.pekko.cluster.ddata.LWWRegister;
import org.apache.pekko.cluster.ddata.SelfUniqueAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import com.example.pekko.RedisPublisher;
import com.example.pekko.FxRateGrpcServer;
import com.typesafe.config.Config;
import com.example.pekko.model.FxRate;

import static com.example.pekko.FxRateStreamProcessor.FX_RATES_KEY;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        log.info("Starting FxRate Kafka to Redis/ORMultiMap streaming application with HTTP server");
        
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "FxRateProcessor");
        
        try {
            // Initialize distributed data components
            ActorRef<Replicator.Command> replicator = DistributedData.get(system).replicator();
            SelfUniqueAddress node = DistributedData.get(system).selfUniqueAddress();
            ObjectMapper objectMapper = new ObjectMapper();
            FxRateTimestampExtractor timestampExtractor = FxRateTimestampExtractor.getConfigured();


            // Rehydrate LWWMap from Redis before serving any data
            RedisPublisher redisPublisher = new RedisPublisher();
            try {
                var initialRates = redisPublisher.loadAllFxRates()
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
                for (FxRate rate : initialRates) {
                    try {
                        String currencyPair = rate.getFromCurrency() + "_" + rate.getToCurrency();
                        long timestamp = timestampExtractor.apply(rate);

                        replicator.tell(new Replicator.Update<>(
                                FX_RATES_KEY,
                                LWWMap.empty(),
                                Replicator.writeLocal(),
                                null,
                                curr -> curr.put(node, currencyPair, rate, new LWWRegister.Clock<FxRate>() {
                                    @Override
                                    public long apply(long currentTimestamp, FxRate value) {
                                        return timestamp; // Use our configurable timestamp
                                    }
                                })));
                        
                        log.debug("Rehydrated FX rate: {} = {} (timestamp: {})", currencyPair, rate.getId(), timestamp);
                    } catch (Exception e) {
                        log.warn("Failed to seed FX rate from Redis: {}", rate.getId(), e);
                    }
                }
                log.info("Rehydrated {} FX rates from Redis to LWWMap", initialRates.size());
            } catch (Exception e) {
                log.error("Failed to rehydrate LWWMap from Redis, exiting", e);
                system.terminate();
                return;
            }

            // Note: HTTP server would need to be updated to use LWWMap directly
            // For now, we skip the HTTP server to focus on gRPC and stream processing
            
            // Start the gRPC server for real-time FX rate streaming
            Config grpcConfig = system.settings().config().getConfig("app.grpc-server");
            int grpcPort = grpcConfig.getInt("port");
            FxRateGrpcServer grpcServer = new FxRateGrpcServer(system);
            grpcServer.start(grpcPort);

            // Create and start the stream processor
            FxRateStreamProcessor processor = new FxRateStreamProcessor(system, redisPublisher);
            CompletionStage<Done> streamCompletion = processor.startProcessing();
            
            streamCompletion.whenComplete((done, throwable) -> {
                if (throwable != null) {
                    log.error("Stream processing failed", throwable);
                } else {
                    log.info("Stream processing completed successfully");
                }
            });
            
            log.info("FxRate application started successfully");
            log.info("gRPC streaming endpoint available at port {}", grpcPort);
            log.info("Using Pekko distributed data LWWMap for storage with {} timestamp extractor", timestampExtractor);
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down application");
                grpcServer.stop();
                redisPublisher.close();
                system.terminate();
            }));
            
        } catch (Exception e) {
            log.error("Failed to start application", e);
            system.terminate();
        }
    }
    
}
