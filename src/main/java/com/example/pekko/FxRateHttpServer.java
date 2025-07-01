package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.model.ws.Message;
import org.apache.pekko.http.javadsl.model.ws.TextMessage;
import org.apache.pekko.http.javadsl.model.ws.WebSocketUpgrade;
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.japi.function.Function;
import org.apache.pekko.stream.javadsl.Flow;
import org.apache.pekko.stream.javadsl.Sink;
import org.apache.pekko.stream.javadsl.Source;
import org.apache.pekko.util.Timeout;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static org.apache.pekko.http.javadsl.server.Directives.*;
import org.apache.pekko.NotUsed;
import org.apache.pekko.japi.JavaPartialFunction;
import org.apache.pekko.stream.OverflowStrategy;
import org.apache.pekko.stream.javadsl.Keep;
import org.apache.pekko.stream.javadsl.BroadcastHub;
import org.apache.pekko.stream.CompletionStrategy;
import java.util.Optional;

public class FxRateHttpServer extends AbstractBehavior<FxRateHttpServer.Command> {
    private static final Logger log = LoggerFactory.getLogger(FxRateHttpServer.class);
    
    public interface Command {}
    
    public static final class StartServer implements Command {
        public final String host;
        public final int port;
        public final ActorRef<ServerStarted> replyTo;
        
        public StartServer(String host, int port, ActorRef<ServerStarted> replyTo) {
            this.host = host;
            this.port = port;
            this.replyTo = replyTo;
        }
    }
    
    public static final class ServerStarted implements Command {
        public final ServerBinding binding;
        
        public ServerStarted(ServerBinding binding) {
            this.binding = binding;
        }
    }
    
    public static final class StopServer implements Command {}
    
    private final ActorRef<FxRateStorage.Command> fxRateStorage;
    private final ObjectMapper objectMapper;
    private final ActorSystem<?> system;
    private ServerBinding binding;
    
    public static Behavior<Command> create(ActorRef<FxRateStorage.Command> fxRateStorage) {
        return Behaviors.setup(context -> new FxRateHttpServer(context, fxRateStorage));
    }
    
    private FxRateHttpServer(ActorContext<Command> context, ActorRef<FxRateStorage.Command> fxRateStorage) {
        super(context);
        this.fxRateStorage = fxRateStorage;
        this.objectMapper = new ObjectMapper();
        this.system = context.getSystem();
        
        log.info("FxRateHttpServer actor started");
    }
    
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartServer.class, this::startServer)
                .onMessage(StopServer.class, this::stopServer)
                .onMessage(ServerStarted.class, this::onServerStarted)
                .build();
    }
    
    private Behavior<Command> startServer(StartServer command) {
        Route routes = createRoutes();
        
        CompletionStage<ServerBinding> bindingFuture = Http.get(system)
                .newServerAt(command.host, command.port)
                .bind(routes);
        
        bindingFuture.whenComplete((binding, throwable) -> {
            if (throwable == null) {
                log.info("FxRate HTTP server started at {}:{}", command.host, command.port);
                getContext().getSelf().tell(new ServerStarted(binding));
                if (command.replyTo != null) {
                    command.replyTo.tell(new ServerStarted(binding));
                }
            } else {
                log.error("Failed to start FxRate HTTP server", throwable);
            }
        });
        
        return this;
    }
    
    private Behavior<Command> onServerStarted(ServerStarted serverStarted) {
        this.binding = serverStarted.binding;
        return this;
    }
    
    private Behavior<Command> stopServer(StopServer command) {
        if (binding != null) {
            binding.unbind().whenComplete((done, throwable) -> {
                if (throwable == null) {
                    log.info("FxRate HTTP server stopped");
                } else {
                    log.error("Error stopping FxRate HTTP server", throwable);
                }
            });
        }
        return Behaviors.stopped();
    }
    
    private Route createRoutes() {
        return concat(
                // WebSocket route for real-time FX rate updates
                path("ws", () ->
                        handleWebSocketMessages(createWebSocketFlow())
                ),
                
                // REST API routes
                pathPrefix("api", () -> concat(
                        path("fxrates", () ->
                                get(() -> 
                                        parameter("pair", pair -> {
                                            if (pair.isEmpty()) {
                                                return getAllFxRates();
                                            } else {
                                                return getFxRatesByPair(pair);
                                            }
                                        })
                                )
                        ),
                        path("fxrates", () ->
                                get(() -> getAllFxRates())
                        )
                )),
                
                // Health check endpoint
                path("health", () ->
                        get(() -> complete(StatusCodes.OK, "FxRate service is healthy"))
                ),
                
                // Static content or default route
                pathSingleSlash(() ->
                        get(() -> complete(StatusCodes.OK, 
                                "FxRate WebSocket Service\n" +
                                "WebSocket: /ws\n" +
                                "REST API: /api/fxrates[?pair=USD_EUR]\n" +
                                "Health: /health"))
                )
        );
    }
    
    private Flow<Message, Message, NotUsed> createWebSocketFlow() {
        log.info("Creating WebSocket flow with real-time FX rate subscription");
        
        // Create a queue source for FX rate updates  
        var queueWithSource = Source.<FxRateStorage.FxRateUpdate>queue(100, OverflowStrategy.dropHead())
                .preMaterialize(system);
        
        var sourceQueue = queueWithSource.first();
        var fxRateUpdateSource = queueWithSource.second();
        
        // Create a subscriber behavior for WebSocket updates
        Behavior<FxRateStorage.FxRateUpdate> subscriberBehavior = Behaviors.receive((context, update) -> {
            log.debug("WebSocket subscriber received FX rate update: {}", update.currencyPair);
            sourceQueue.offer(update);
            return Behaviors.same();
        });
        
        // Create a unique subscriber name for this WebSocket connection
        String subscriberName = "websocket-subscriber-" + System.currentTimeMillis();
        
        // Spawn the subscriber and subscribe to updates
        // We need to do this in the context of creating the WebSocket flow
        ActorRef<FxRateStorage.FxRateUpdate> subscriber = system.systemActorOf(
                subscriberBehavior, 
                subscriberName, 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Subscribe to FX rate updates
        fxRateStorage.tell(new FxRateStorage.Subscribe(subscriber));
        
        // Convert FX rate updates to WebSocket messages
        Source<Message, NotUsed> fxRateSource = fxRateUpdateSource
                .map(this::convertFxRateUpdateToMessage);
        
        // Handle incoming WebSocket messages
        Flow<Message, Message, NotUsed> incomingMessageFlow = Flow.<Message>create()
                .collect(new JavaPartialFunction<Message, Message>() {
                    @Override
                    public Message apply(Message msg, boolean isCheck) throws Exception {
                        if (isCheck) {
                            return msg.isText() ? null : null;
                        } else {
                            return handleWebSocketMessage(msg);
                        }
                    }
                });
        
        // Merge all sources: incoming messages, FX rate updates, and heartbeat
        return incomingMessageFlow
                .merge(fxRateSource, true)
                .merge(createHeartbeatSource(), true);
    }
    
    private Message handleWebSocketMessage(Message message) {
        if (message.isText()) {
            TextMessage textMessage = message.asTextMessage();
            if (textMessage.isStrict()) {
                String text = textMessage.getStrictText();
                log.debug("Received WebSocket message: {}", text);
                
                // Check if client wants to create a test FX rate
                if (text.startsWith("createFxRate:")) {
                    String[] parts = text.split(":");
                    if (parts.length >= 2) {
                        String pair = parts[1];
                        createTestFxRate(pair);
                        String response = String.format(
                            "{\"type\": \"fxRateCreated\", \"pair\": \"%s\", \"timestamp\": %d}", 
                            pair, System.currentTimeMillis()
                        );
                        return TextMessage.create(response);
                    }
                }
                
                // Regular echo functionality
                String response = String.format(
                    "{\"type\": \"echo\", \"message\": \"%s\", \"timestamp\": %d}", 
                    text, System.currentTimeMillis()
                );
                return TextMessage.create(response);
            } else {
                // Handle streamed text messages
                return TextMessage.create(
                    Source.single("Echo: ").concat(textMessage.getStreamedText())
                );
            }
        }
        // For non-text messages, just echo them back
        return message;
    }
    
    private void createTestFxRate(String pair) {
        try {
            String[] currencies = pair.split("_");
            if (currencies.length == 2) {
                com.example.pekko.model.FxRate testFxRate = com.example.pekko.model.FxRate.newBuilder()
                        .setId("ws-test-" + System.currentTimeMillis())
                        .setFromCurrency(currencies[0])
                        .setToCurrency(currencies[1])
                        .setRate(Math.random() * 2.0) // Random rate between 0 and 2
                        .setTimestamp(System.currentTimeMillis())
                        .setSource("WEBSOCKET_TEST")
                        .build();
                
                // Store the FX rate (this will trigger notifications to subscribers)
                Duration timeout = Duration.ofSeconds(5);
                org.apache.pekko.actor.typed.javadsl.AskPattern.<FxRateStorage.Command, FxRateStorage.StoreResponse>ask(
                        fxRateStorage,
                        replyTo -> new FxRateStorage.StoreFxRate(testFxRate, replyTo),
                        timeout,
                        system.scheduler()
                ).whenComplete((response, throwable) -> {
                    if (throwable == null && response.success) {
                        log.info("Successfully created test FX rate via WebSocket: {}", pair);
                    } else {
                        log.error("Failed to create test FX rate via WebSocket", throwable);
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error creating test FX rate", e);
        }
    }
    
    private Message convertFxRateUpdateToMessage(FxRateStorage.FxRateUpdate update) {
        try {
            String json = String.format(
                "{\"type\": \"fxRateUpdate\", \"currencyPair\": \"%s\", \"id\": \"%s\", \"fromCurrency\": \"%s\", \"toCurrency\": \"%s\", \"rate\": %f, \"timestamp\": %d, \"source\": \"%s\"}", 
                update.currencyPair,
                update.fxRate.getId(),
                update.fxRate.getFromCurrency(),
                update.fxRate.getToCurrency(),
                update.fxRate.getRate(),
                update.fxRate.getTimestamp(),
                update.fxRate.getSource() != null ? update.fxRate.getSource() : "UNKNOWN"
            );
            log.debug("Sending FX rate update via WebSocket: {}", json);
            return TextMessage.create(json);
        } catch (Exception e) {
            log.error("Failed to convert FX rate update to message", e);
            return TextMessage.create("{\"type\": \"error\", \"message\": \"Failed to serialize FX rate update\"}");
        }
    }
    
    private Source<Message, ?> createHeartbeatSource() {
        return Source.tick(
                Duration.ofSeconds(2),    // Initial delay
                Duration.ofSeconds(30),   // Send heartbeat every 30 seconds (less frequent now that we have real data)
                "heartbeat"
        ).map(tick -> {
            String heartbeat = String.format(
                "{\"type\": \"heartbeat\", \"timestamp\": %d}", 
                System.currentTimeMillis()
            );
            return (Message) TextMessage.create(heartbeat);
        });
    }
    
    
    
    private Route getAllFxRates() {
        Duration timeout = Duration.ofSeconds(5);
        
        return onComplete(
                org.apache.pekko.actor.typed.javadsl.AskPattern.<FxRateStorage.Command, FxRateStorage.GetAllResponse>ask(
                        fxRateStorage,
                        replyTo -> new FxRateStorage.GetAllFxRates(replyTo),
                        timeout,
                        system.scheduler()
                ),
                result -> {
                    if (result.isSuccess()) {
                        FxRateStorage.GetAllResponse response = (FxRateStorage.GetAllResponse) result.get();
                        if (response.success) {
                            try {
                                String json = objectMapper.writeValueAsString(response.allFxRates);
                                return complete(StatusCodes.OK, json);
                            } catch (Exception e) {
                                log.error("Failed to serialize FX rates", e);
                                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Serialization error");
                            }
                        } else {
                            return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Failed to retrieve FX rates");
                        }
                    } else {
                        log.error("Failed to get FX rates", result.failed().get());
                        return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Internal error");
                    }
                }
        );
    }
    
    private Route getFxRatesByPair(String pair) {
        Duration timeout = Duration.ofSeconds(5);
        
        return onComplete(
                org.apache.pekko.actor.typed.javadsl.AskPattern.<FxRateStorage.Command, FxRateStorage.GetResponse>ask(
                        fxRateStorage,
                        replyTo -> new FxRateStorage.GetFxRates(pair, replyTo),
                        timeout,
                        system.scheduler()
                ),
                result -> {
                    if (result.isSuccess()) {
                        FxRateStorage.GetResponse response = (FxRateStorage.GetResponse) result.get();
                        if (response.success) {
                            try {
                                String json = objectMapper.writeValueAsString(response.fxRates);
                                return complete(StatusCodes.OK, json);
                            } catch (Exception e) {
                                log.error("Failed to serialize FX rates for pair: {}", pair, e);
                                return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Serialization error");
                            }
                        } else {
                            return complete(StatusCodes.NOT_FOUND, "FX rates not found for pair: " + pair);
                        }
                    } else {
                        log.error("Failed to get FX rates for pair: {}", pair, result.failed().get());
                        return complete(StatusCodes.INTERNAL_SERVER_ERROR, "Internal error");
                    }
                }
        );
    }
}