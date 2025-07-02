package com.example.pekko.client;

import com.example.pekko.grpc.FxRateServiceGrpc;
import com.example.pekko.grpc.FxRateWithRiskMessage;
import com.example.pekko.grpc.SubscribeRequest;
import com.example.pekko.grpc.TradeRiskMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * gRPC client that subscribes to FX rate updates WITH risk calculations.
 * This client receives not just the updated FX rate, but also calculated risks
 * for all trades that use that currency pair (risk = notional * rate).
 */
public class GrpcCliAll {
    private static final Logger log = LoggerFactory.getLogger(GrpcCliAll.class);

    public static void main(String[] args) {
        String serverHost = "localhost";
        int serverPort = 50051;
        
        if (args.length >= 1) {
            serverHost = args[0];
        }
        if (args.length >= 2) {
            serverPort = Integer.parseInt(args[1]);
        }

        log.info("Starting GrpcCliAll - connecting to {}:{}", serverHost, serverPort);
        log.info("This client will receive FX rate updates WITH risk calculations for affected trades");

        // Create channel and stub
        ManagedChannel channel = ManagedChannelBuilder.forAddress(serverHost, serverPort)
                .usePlaintext()
                .build();

        FxRateServiceGrpc.FxRateServiceStub stub = FxRateServiceGrpc.newStub(channel);

        // Latch to keep the main thread alive
        CountDownLatch latch = new CountDownLatch(1);

        // Create stream observer for receiving FX rate updates with risk calculations
        StreamObserver<FxRateWithRiskMessage> responseObserver = new StreamObserver<FxRateWithRiskMessage>() {
            @Override
            public void onNext(FxRateWithRiskMessage riskMessage) {
                var fxRate = riskMessage.getFxRate();
                var tradeRisks = riskMessage.getTradeRisksList();
                
                log.info("=== FX RATE UPDATE WITH RISK CALCULATIONS ===");
                log.info("FX Rate: {} {} -> {} = {} (source: {}, timestamp: {})",
                        fxRate.getId(),
                        fxRate.getFromCurrency(),
                        fxRate.getToCurrency(),
                        fxRate.getRate(),
                        fxRate.getSource(),
                        fxRate.getTimestamp());
                
                log.info("Risk calculations for {} affected trades:", tradeRisks.size());
                
                double totalRisk = 0.0;
                for (TradeRiskMessage tradeRisk : tradeRisks) {
                    var trade = tradeRisk.getTrade();
                    double risk = tradeRisk.getRisk();
                    double rateUsed = tradeRisk.getRateUsed();
                    
                    log.info("  Trade {}: {} {} -> {} | Notional: {} | Rate: {} | RISK: {}",
                            trade.getTradeId(),
                            trade.getFromCurrency(),
                            trade.getToCurrency(),
                            trade.getNotional(),
                            rateUsed,
                            risk);
                    
                    totalRisk += risk;
                }
                
                log.info("TOTAL RISK EXPOSURE: {}", totalRisk);
                log.info("===============================================");
            }

            @Override
            public void onError(Throwable t) {
                log.error("Error receiving FX rate updates with risk calculations", t);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                log.info("FX rate with risk calculations stream completed");
                latch.countDown();
            }
        };

        // Subscribe to FX rate updates with risk calculations
        log.info("Subscribing to FX rate updates with risk calculations...");
        stub.subscribeRatesWithRisk(SubscribeRequest.newBuilder().build(), responseObserver);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down GrpcCliAll...");
            channel.shutdown();
            latch.countDown();
        }));

        try {
            // Wait indefinitely for updates
            latch.await();
        } catch (InterruptedException e) {
            log.info("Client interrupted");
            Thread.currentThread().interrupt();
        } finally {
            channel.shutdown();
            log.info("GrpcCliAll stopped");
        }
    }
}