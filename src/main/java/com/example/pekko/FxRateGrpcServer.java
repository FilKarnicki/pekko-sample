package com.example.pekko;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.ddata.typed.javadsl.DistributedData;
import org.apache.pekko.cluster.ddata.typed.javadsl.Replicator;
import org.apache.pekko.cluster.ddata.LWWMap;
import org.apache.pekko.cluster.ddata.Key;
import com.example.pekko.grpc.FxRateMessage;
import com.example.pekko.grpc.FxRateServiceGrpc;
import com.example.pekko.grpc.SubscribeRequest;
import com.example.pekko.model.FxRate;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final Key<LWWMap<String, FxRateMessage>> FX_RATES_KEY = FxRateStreamProcessor.FX_RATES_KEY;
    
    private final ActorSystem<?> system;
    private final ActorRef<Replicator.Command> replicator;
    private final ObjectMapper objectMapper;
    private Server server;
    private ActorRef<StreamSubscriberActor.Command> subscriberActor;

    public FxRateGrpcServer(ActorSystem<?> system) {
        this.system = system;
        this.replicator = DistributedData.get(system).replicator();
        this.objectMapper = new ObjectMapper();
        
        // Create the subscriber actor that will handle LWWMap changes
        this.subscriberActor = system.systemActorOf(
                StreamSubscriberActor.create(replicator),
                "fx-rate-subscriber",
                org.apache.pekko.actor.typed.Props.empty());
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
            log.info("gRPC client subscribed to FX rate updates from LWWMap");
            
            // Register this stream observer with the subscriber actor
            subscriberActor.tell(new StreamSubscriberActor.RegisterStream(responseObserver));
            
            log.info("LWWMap-based gRPC service is ready. Use timestamp extractor: {}", 
                     FxRateTimestampExtractor.getConfigured());
            
            // Note: Stream is kept open - responseObserver.onCompleted() will be called
            // when the client disconnects or an error occurs
        }
    }
    
    /**
     * Actor that subscribes to LWWMap changes and forwards them to gRPC stream observers
     */
    public static class StreamSubscriberActor extends AbstractBehavior<StreamSubscriberActor.Command> {
        private static final Logger log = LoggerFactory.getLogger(StreamSubscriberActor.class);
        
        public interface Command {}
        
        public static final class RegisterStream implements Command {
            public final StreamObserver<FxRateMessage> observer;
            
            public RegisterStream(StreamObserver<FxRateMessage> observer) {
                this.observer = observer;
            }
        }
        
        public static final class UnregisterStream implements Command {
            public final StreamObserver<FxRateMessage> observer;
            
            public UnregisterStream(StreamObserver<FxRateMessage> observer) {
                this.observer = observer;
            }
        }
        
        private static final class FxRateUpdateAdapter implements Command {
            public final FxRateStreamProcessor.FxRateUpdated update;
            
            public FxRateUpdateAdapter(FxRateStreamProcessor.FxRateUpdated update) {
                this.update = update;
            }
        }
        
        private final ActorRef<Replicator.Command> replicator;
        private final Set<StreamObserver<FxRateMessage>> activeStreams = ConcurrentHashMap.newKeySet();
        
        public static Behavior<Command> create(ActorRef<Replicator.Command> replicator) {
            return Behaviors.setup(context -> new StreamSubscriberActor(context, replicator));
        }
        
        private StreamSubscriberActor(ActorContext<Command> context, ActorRef<Replicator.Command> replicator) {
            super(context);
            this.replicator = replicator;
            
            // BETTER APPROACH: Subscribe to direct FX rate update events instead of LWWMap changes
            // This eliminates the need to track previous state or compute deltas!
            ActorRef<FxRateStreamProcessor.FxRateUpdated> updateAdapter = 
                    context.messageAdapter(FxRateStreamProcessor.FxRateUpdated.class, FxRateUpdateAdapter::new);
            
            context.getSystem().eventStream().tell(new org.apache.pekko.actor.typed.eventstream.EventStream.Subscribe<>(
                    FxRateStreamProcessor.FxRateUpdated.class, updateAdapter));
            
            log.info("Subscribed to direct FX rate update events for gRPC streaming");
        }
        
        @Override
        public Receive<Command> createReceive() {
            return newReceiveBuilder()
                    .onMessage(RegisterStream.class, this::onRegisterStream)
                    .onMessage(UnregisterStream.class, this::onUnregisterStream)
                    .onMessage(FxRateUpdateAdapter.class, this::onFxRateUpdate)
                    .build();
        }
        
        private Behavior<Command> onRegisterStream(RegisterStream command) {
            activeStreams.add(command.observer);
            log.info("Registered new gRPC stream observer. Total active streams: {}. Will receive real-time updates.", activeStreams.size());
            return this;
        }
        
        private Behavior<Command> onUnregisterStream(UnregisterStream command) {
            activeStreams.remove(command.observer);
            log.info("Unregistered gRPC stream observer. Total active streams: {}", activeStreams.size());
            return this;
        }
        
        private Behavior<Command> onFxRateUpdate(FxRateUpdateAdapter adapter) {
            FxRateStreamProcessor.FxRateUpdated update = adapter.update;
            FxRateMessage fxRate = update.fxRateMessage;
            
            log.debug("Received direct FX rate update: {} = {}, broadcasting to {} active streams", 
                     update.currencyPair, fxRate.getId(), activeStreams.size());

            // Broadcast the single changed entry to all active streams
            Set<StreamObserver<FxRateMessage>> streamsToRemove = new HashSet<>();
            for (StreamObserver<FxRateMessage> observer : activeStreams) {
                try {
                    observer.onNext(fxRate);
                } catch (Exception e) {
                    log.warn("Failed to send update to gRPC stream, removing observer", e);
                    streamsToRemove.add(observer);
                }
            }

            // Remove failed streams
            activeStreams.removeAll(streamsToRemove);

            return this;
        }
        
        
        public static FxRateMessage convertToGrpcMessage(FxRate fxRate, String source) {
            return FxRateMessage.newBuilder()
                    .setId(fxRate.getId().toString())
                    .setFromCurrency(fxRate.getFromCurrency().toString())
                    .setToCurrency(fxRate.getToCurrency().toString())
                    .setRate(fxRate.getRate())
                    .setTimestamp(fxRate.getTimestamp())
                    .setSource(source)
                    .build();
        }
    }
}