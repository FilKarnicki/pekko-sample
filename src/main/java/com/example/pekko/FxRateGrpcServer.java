package com.example.pekko;

import java.io.IOException;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import com.example.pekko.grpc.FxRateMessage;
import com.example.pekko.grpc.FxRateServiceGrpc;
import com.example.pekko.grpc.SubscribeRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC server that streams FX rate updates to subscribed clients.
 */
public class FxRateGrpcServer {
    private static final Logger log = LoggerFactory.getLogger(FxRateGrpcServer.class);
    private final ActorSystem<?> system;
    private final ActorRef<FxRateStorage.Command> storage;
    private Server server;

    public FxRateGrpcServer(ActorSystem<?> system, ActorRef<FxRateStorage.Command> storage) {
        this.system = system;
        this.storage = storage;
    }

    /**
     * Starts the gRPC server on the given port.
     */
    public void start(int port) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new FxRateServiceImpl())
                .build()
                .start();
        log.info("gRPC server started, listening on port {}", port);
    }

    /**
     * Stops the gRPC server.
     */
    public void stop() {
        if (server != null) {
            server.shutdown();
            log.info("gRPC server stopped");
        }
    }

    private class FxRateServiceImpl extends FxRateServiceGrpc.FxRateServiceImplBase {
        @Override
        public void subscribeRates(SubscribeRequest request, StreamObserver<FxRateMessage> responseObserver) {
            // Spawn an actor to forward storage updates to the gRPC stream
            Behavior<FxRateStorage.FxRateUpdate> behavior = Behaviors.receiveMessage(update -> {
                FxRateMessage msg = FxRateMessage.newBuilder()
                        .setId(update.fxRate.getId().toString())
                        .setFromCurrency(update.fxRate.getFromCurrency().toString())
                        .setToCurrency(update.fxRate.getToCurrency().toString())
                        .setRate(update.fxRate.getRate())
                        .setTimestamp(update.fxRate.getTimestamp())
                        .setSource(update.fxRate.getSource() != null ? update.fxRate.getSource().toString() : "")
                        .build();
                responseObserver.onNext(msg);
                return Behaviors.same();
            });
            ActorRef<FxRateStorage.FxRateUpdate> subscriber = system.systemActorOf(
                    behavior,
                    "grpc-subscriber-" + System.currentTimeMillis(),
                    org.apache.pekko.actor.typed.Props.empty());

            // Subscribe to FX rate updates
            storage.tell(new FxRateStorage.Subscribe(subscriber));
            // Note: Stream stays open until client cancels or server stops
        }
    }
}