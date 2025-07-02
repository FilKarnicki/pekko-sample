package com.example.pekko.client;

import com.example.pekko.grpc.FxRateMessage;
import com.example.pekko.grpc.FxRateServiceGrpc;
import com.example.pekko.grpc.SubscribeRequest;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

/**
 * A simple gRPC client that connects to the FxRate gRPC server and prints FX rate updates.
 */
public class GrpcCli {
    public static void main(String[] args) throws InterruptedException {
        String host = "localhost";
        int port = 50051;
        if (args.length >= 2) {
            host = args[0];
            port = Integer.parseInt(args[1]);
        }
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        FxRateServiceGrpc.FxRateServiceStub stub = FxRateServiceGrpc.newStub(channel);
        stub.subscribeRates(SubscribeRequest.newBuilder().build(), new StreamObserver<FxRateMessage>() {
            @Override
            public void onNext(FxRateMessage value) {
                System.out.println("Received FX rate update: " + value);
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Error in gRPC stream: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("gRPC stream completed");
            }
        });
        // Keep the client running to receive streamed updates
        Thread.currentThread().join();
    }
}