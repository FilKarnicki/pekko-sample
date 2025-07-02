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
import org.apache.pekko.http.javadsl.server.AllDirectives;
import org.apache.pekko.http.javadsl.server.Route;
import org.apache.pekko.util.Timeout;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import static org.apache.pekko.http.javadsl.server.Directives.*;

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
                                "FxRate gRPC Service\n" +
                                "REST API: /api/fxrates[?pair=USD_EUR]\n" +
                                "Health: /health"))
                )
        );
    }
    
    // WebSocket support removed; use gRPC streaming via FxRateGrpcServer
    
    
    
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