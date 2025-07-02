package com.example.pekko;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
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
    
    private final ActorSystem<?> system;
    private final ActorRef<Replicator.Command> replicator;
    private final ObjectMapper objectMapper;
    private Server server;

    public FxRateGrpcServer(ActorSystem<?> system) {
        this.system = system;
        this.replicator = DistributedData.get(system).replicator();
        this.objectMapper = new ObjectMapper();
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
            
            // For demonstration, send a message indicating LWWMap is being used
            FxRateMessage demoMsg = FxRateMessage.newBuilder()
                    .setId("lww-demo-001")
                    .setFromCurrency("USD")
                    .setToCurrency("EUR")
                    .setRate(0.85)
                    .setTimestamp(System.currentTimeMillis())
                    .setSource("LWWMap Demo")
                    .build();
            responseObserver.onNext(demoMsg);
            
            log.info("LWWMap-based gRPC service is ready. Use timestamp extractor: {}", 
                     FxRateTimestampExtractor.getConfigured());
            
            // Note: In a full implementation, you would:
            // 1. Subscribe to LWWMap changes via replicator.tell(new Replicator.Subscribe<>(...))
            // 2. Forward updates to the responseObserver
            // 3. Keep the stream open for real-time updates
            
            responseObserver.onCompleted();
        }
    }
}