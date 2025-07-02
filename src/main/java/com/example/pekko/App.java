package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.Done;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.time.Duration;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import com.example.pekko.RedisPublisher;
import com.example.pekko.FxRateGrpcServer;
import com.typesafe.config.Config;
import com.example.pekko.model.FxRate;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        log.info("Starting FxRate Kafka to Redis/ORMultiMap streaming application with HTTP server");
        
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "FxRateProcessor");
        
        try {
            // Create the FxRate storage actor
            ActorRef<FxRateStorage.Command> fxRateStorage = system.systemActorOf(
                    FxRateStorage.create(), "fx-rate-storage", org.apache.pekko.actor.typed.Props.empty());

            // Rehydrate storage from Redis before serving any data
            RedisPublisher redisPublisher = new RedisPublisher();
            try {
                var initialRates = redisPublisher.loadAllFxRates()
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
                for (FxRate rate : initialRates) {
                    FxRateStorage.StoreResponse resp = AskPattern.<FxRateStorage.Command, FxRateStorage.StoreResponse>ask(
                            fxRateStorage,
                            replyTo -> new FxRateStorage.StoreFxRate(rate, replyTo),
                            Duration.ofSeconds(5),
                            system.scheduler()
                    ).toCompletableFuture().get(5, TimeUnit.SECONDS);
                    if (!resp.success) {
                        log.warn("Failed to seed FX rate from Redis: {}", rate.getId());
                    }
                }
                log.info("Rehydrated {} FX rates from Redis", initialRates.size());
            } catch (Exception e) {
                log.error("Failed to rehydrate fxRateStorage from Redis, exiting", e);
                system.terminate();
                return;
            }

            // Create the HTTP server actor
            ActorRef<FxRateHttpServer.Command> httpServer = system.systemActorOf(
                    FxRateHttpServer.create(fxRateStorage), "fx-rate-http-server", org.apache.pekko.actor.typed.Props.empty());

            // Start the HTTP server
            httpServer.tell(new FxRateHttpServer.StartServer("localhost", 8080, null));

            // Start the gRPC server for real-time FX rate streaming
            Config grpcConfig = system.settings().config().getConfig("app.grpc-server");
            int grpcPort = grpcConfig.getInt("port");
            FxRateGrpcServer grpcServer = new FxRateGrpcServer(system, fxRateStorage);
            grpcServer.start(grpcPort);

            // Create and start the stream processor
            FxRateStreamProcessor processor = new FxRateStreamProcessor(system, fxRateStorage, redisPublisher);
            CompletionStage<Done> streamCompletion = processor.startProcessing();
            
            streamCompletion.whenComplete((done, throwable) -> {
                if (throwable != null) {
                    log.error("Stream processing failed", throwable);
                } else {
                    log.info("Stream processing completed successfully");
                }
            });
            
            log.info("FxRate application started successfully");
            log.info("HTTP server available at: http://localhost:8080");
            log.info("gRPC streaming endpoint available at port {}", grpcPort);
            log.info("REST API: http://localhost:8080/api/fxrates");
            log.info("Health check: http://localhost:8080/health");
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down application");
                httpServer.tell(new FxRateHttpServer.StopServer());
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
