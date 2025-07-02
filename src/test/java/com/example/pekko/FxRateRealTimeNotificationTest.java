package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import com.example.pekko.model.FxRate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Disabled("WebSocket support removed; use gRPC client")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FxRateRealTimeNotificationTest {
    private static final Logger log = LoggerFactory.getLogger(FxRateRealTimeNotificationTest.class);
    
    private static ActorSystem<Void> system;
    private static ActorRef<FxRateStorage.Command> fxRateStorage;
    private static ActorRef<FxRateHttpServer.Command> httpServer;
    private static ObjectMapper objectMapper;
    private static final int TEST_PORT = 18082; // Use different port for real-time testing
    private static final String WS_URL = "ws://localhost:" + TEST_PORT + "/ws";

    @BeforeAll
    static void setup() throws Exception {
        log.info("Setting up real-time notification test");
        
        // Create actor system with test configuration
        com.typesafe.config.Config testConfig = com.typesafe.config.ConfigFactory.load("application-test");
        system = ActorSystem.create(Behaviors.empty(), "FxRateRealTimeTest", testConfig);
        
        // Create storage actor
        fxRateStorage = system.systemActorOf(
                FxRateStorage.create(), 
                "fx-rate-storage-realtime-test", 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Create HTTP server actor
        httpServer = system.systemActorOf(
                FxRateHttpServer.create(fxRateStorage), 
                "fx-rate-http-server-realtime-test", 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Start server and wait for it to be ready
        CompletableFuture<Void> serverStarted = new CompletableFuture<>();
        
        ActorRef<FxRateHttpServer.ServerStarted> replyTo = system.systemActorOf(
                Behaviors.receive((context, message) -> {
                    serverStarted.complete(null);
                    return Behaviors.stopped();
                }),
                "realtime-server-started-reply",
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        httpServer.tell(new FxRateHttpServer.StartServer("localhost", TEST_PORT, replyTo));
        serverStarted.get(10, TimeUnit.SECONDS);
        
        objectMapper = new ObjectMapper();
        
        log.info("Real-time test server started on port {}", TEST_PORT);
        
        // Give server a moment to fully initialize
        Thread.sleep(1000);
    }
    
    @AfterAll
    static void teardown() throws Exception {
        log.info("Tearing down real-time notification test");
        
        if (system != null) {
            httpServer.tell(new FxRateHttpServer.StopServer());
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
    
    @Test
    @Order(1)
    void testWebSocketReceivesExternalFxRateUpdates() throws Exception {
        log.info("Testing WebSocket receives external FX rate updates");
        
        CountDownLatch connectionLatch = new CountDownLatch(1);
        CountDownLatch fxRateUpdateLatch = new CountDownLatch(1);
        AtomicReference<String> receivedFxRateUpdate = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        
        WebSocketClient client = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("WebSocket connection opened for external update test");
                connectionLatch.countDown();
            }
            
            @Override
            public void onMessage(String message) {
                log.info("Received message: {}", message);
                try {
                    JsonNode json = objectMapper.readTree(message);
                    if ("fxRateUpdate".equals(json.get("type").asText())) {
                        receivedFxRateUpdate.set(message);
                        fxRateUpdateLatch.countDown();
                    }
                } catch (Exception e) {
                    log.error("Error parsing message", e);
                    error.set(e);
                    fxRateUpdateLatch.countDown();
                }
            }
            
            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed for external update test: {} - {}", code, reason);
            }
            
            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error in external update test", ex);
                error.set(ex);
                connectionLatch.countDown();
                fxRateUpdateLatch.countDown();
            }
        };
        
        try {
            // Connect WebSocket client
            client.connect();
            assertTrue(connectionLatch.await(10, TimeUnit.SECONDS), "Connection should be established");
            assertNull(error.get(), "No connection error should occur");
            
            // Wait a moment for subscription to be set up
            Thread.sleep(500);
            
            // Now store an FX rate externally (simulating Kafka or other external source)
            FxRate externalFxRate = FxRate.newBuilder()
                    .setId("external-test-" + System.currentTimeMillis())
                    .setFromCurrency("GBP")
                    .setToCurrency("JPY")
                    .setRate(150.75)
                    .setTimestamp(System.currentTimeMillis())
                    .setSource("EXTERNAL_SOURCE")
                    .build();
            
            log.info("Storing external FX rate to trigger WebSocket notification");
            
            // Store the FX rate (this should trigger notifications to WebSocket subscribers)
            CompletableFuture<FxRateStorage.StoreResponse> storeResponse = org.apache.pekko.actor.typed.javadsl.AskPattern
                    .<FxRateStorage.Command, FxRateStorage.StoreResponse>ask(
                            fxRateStorage,
                            replyTo -> new FxRateStorage.StoreFxRate(externalFxRate, replyTo),
                            Duration.ofSeconds(5),
                            system.scheduler()
                    ).toCompletableFuture();
            
            FxRateStorage.StoreResponse response = storeResponse.get(5, TimeUnit.SECONDS);
            assertTrue(response.success, "External FX rate should be stored successfully");
            
            // Wait for WebSocket notification
            assertTrue(fxRateUpdateLatch.await(10, TimeUnit.SECONDS), "Should receive FX rate update notification");
            assertNull(error.get(), "No message error should occur");
            
            String updateMessage = receivedFxRateUpdate.get();
            assertNotNull(updateMessage, "Should receive FX rate update message");
            
            // Parse and verify the FX rate update
            JsonNode json = objectMapper.readTree(updateMessage);
            assertEquals("fxRateUpdate", json.get("type").asText());
            assertEquals("GBP_JPY", json.get("currencyPair").asText());
            assertEquals("GBP", json.get("fromCurrency").asText());
            assertEquals("JPY", json.get("toCurrency").asText());
            assertEquals(150.75, json.get("rate").asDouble(), 0.01);
            assertEquals("EXTERNAL_SOURCE", json.get("source").asText());
            
            log.info("Successfully received real-time FX rate update via WebSocket: {}", updateMessage);
            
        } finally {
            if (client.isOpen()) {
                client.close();
            }
        }
    }
    
    @Test
    @Order(2)
    void testMultipleWebSocketClientsReceiveUpdates() throws Exception {
        log.info("Testing multiple WebSocket clients receive updates");
        
        int numClients = 3;
        CountDownLatch connectionLatch = new CountDownLatch(numClients);
        CountDownLatch updateLatch = new CountDownLatch(numClients);
        AtomicReference<Exception> error = new AtomicReference<>();
        
        WebSocketClient[] clients = new WebSocketClient[numClients];
        
        for (int i = 0; i < numClients; i++) {
            final int clientId = i;
            clients[i] = new WebSocketClient(new URI(WS_URL)) {
                @Override
                public void onOpen(ServerHandshake handshake) {
                    log.info("Client {} connected", clientId);
                    connectionLatch.countDown();
                }
                
                @Override
                public void onMessage(String message) {
                    try {
                        JsonNode json = objectMapper.readTree(message);
                        if ("fxRateUpdate".equals(json.get("type").asText())) {
                            log.info("Client {} received FX rate update: {}", clientId, message);
                            updateLatch.countDown();
                        }
                    } catch (Exception e) {
                        log.error("Client {} error parsing message", clientId, e);
                        error.set(e);
                        updateLatch.countDown();
                    }
                }
                
                @Override
                public void onClose(int code, String reason, boolean remote) {
                    log.info("Client {} disconnected: {} - {}", clientId, code, reason);
                }
                
                @Override
                public void onError(Exception ex) {
                    log.error("Client {} error", clientId, ex);
                    error.set(ex);
                    connectionLatch.countDown();
                    updateLatch.countDown();
                }
            };
        }
        
        try {
            // Connect all clients
            for (WebSocketClient client : clients) {
                client.connect();
            }
            
            assertTrue(connectionLatch.await(10, TimeUnit.SECONDS), "All clients should connect");
            assertNull(error.get(), "No connection errors should occur");
            
            // Wait a moment for subscriptions to be set up
            Thread.sleep(500);
            
            // Store an FX rate that should be broadcast to all clients
            FxRate broadcastFxRate = FxRate.newBuilder()
                    .setId("broadcast-test-" + System.currentTimeMillis())
                    .setFromCurrency("EUR")
                    .setToCurrency("CHF")
                    .setRate(1.08)
                    .setTimestamp(System.currentTimeMillis())
                    .setSource("BROADCAST_TEST")
                    .build();
            
            log.info("Storing FX rate to broadcast to all WebSocket clients");
            
            CompletableFuture<FxRateStorage.StoreResponse> storeResponse = org.apache.pekko.actor.typed.javadsl.AskPattern
                    .<FxRateStorage.Command, FxRateStorage.StoreResponse>ask(
                            fxRateStorage,
                            replyTo -> new FxRateStorage.StoreFxRate(broadcastFxRate, replyTo),
                            Duration.ofSeconds(5),
                            system.scheduler()
                    ).toCompletableFuture();
            
            FxRateStorage.StoreResponse response = storeResponse.get(5, TimeUnit.SECONDS);
            assertTrue(response.success, "Broadcast FX rate should be stored successfully");
            
            // Wait for all clients to receive the update
            assertTrue(updateLatch.await(10, TimeUnit.SECONDS), "All clients should receive FX rate update");
            assertNull(error.get(), "No message errors should occur");
            
            log.info("All {} clients successfully received the FX rate update", numClients);
            
        } finally {
            // Close all clients
            for (WebSocketClient client : clients) {
                if (client != null && client.isOpen()) {
                    client.close();
                }
            }
        }
    }
}