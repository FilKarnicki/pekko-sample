package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.Done;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionStage;

public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        log.info("Starting FxRate Kafka to Redis/ORMultiMap streaming application with HTTP server");
        
        ActorSystem<Void> system = ActorSystem.create(Behaviors.empty(), "FxRateProcessor");
        
        try {
            // Create the FxRate storage actor
            ActorRef<FxRateStorage.Command> fxRateStorage = system.systemActorOf(
                    FxRateStorage.create(), "fx-rate-storage", org.apache.pekko.actor.typed.Props.empty());
            
            // Create the HTTP server actor
            ActorRef<FxRateHttpServer.Command> httpServer = system.systemActorOf(
                    FxRateHttpServer.create(fxRateStorage), "fx-rate-http-server", org.apache.pekko.actor.typed.Props.empty());
            
            // Start the HTTP server
            httpServer.tell(new FxRateHttpServer.StartServer("localhost", 8080, null));
            
            // Create and start the stream processor
            FxRateStreamProcessor processor = new FxRateStreamProcessor(system, fxRateStorage);
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
            log.info("WebSocket endpoint: ws://localhost:8080/ws");
            log.info("REST API: http://localhost:8080/api/fxrates");
            log.info("Health check: http://localhost:8080/health");
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down application");
                httpServer.tell(new FxRateHttpServer.StopServer());
                system.terminate();
            }));
            
        } catch (Exception e) {
            log.error("Failed to start application", e);
            system.terminate();
        }
    }
}
