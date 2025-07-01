package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FxRateWebSocketIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(FxRateWebSocketIntegrationTest.class);
    
    private static ActorSystem<Void> system;
    private static ActorRef<FxRateStorage.Command> fxRateStorage;
    private static ActorRef<FxRateHttpServer.Command> httpServer;
    private static ObjectMapper objectMapper;
    private static final int TEST_PORT = 18081; // Use different port for WebSocket testing
    private static final String WS_URL = "ws://localhost:" + TEST_PORT + "/ws";

    @BeforeAll
    static void setup() throws Exception {
        log.info("Setting up WebSocket integration test");
        
        // Create actor system with test configuration
        com.typesafe.config.Config testConfig = com.typesafe.config.ConfigFactory.load("application-test");
        system = ActorSystem.create(Behaviors.empty(), "FxRateWebSocketTest", testConfig);
        
        // Create storage actor
        fxRateStorage = system.systemActorOf(
                FxRateStorage.create(), 
                "fx-rate-storage-ws-test", 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Create HTTP server actor
        httpServer = system.systemActorOf(
                FxRateHttpServer.create(fxRateStorage), 
                "fx-rate-http-server-ws-test", 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Start server and wait for it to be ready
        CompletableFuture<Void> serverStarted = new CompletableFuture<>();
        
        ActorRef<FxRateHttpServer.ServerStarted> replyTo = system.systemActorOf(
                Behaviors.receive((context, message) -> {
                    serverStarted.complete(null);
                    return Behaviors.stopped();
                }),
                "ws-server-started-reply",
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        httpServer.tell(new FxRateHttpServer.StartServer("localhost", TEST_PORT, replyTo));
        serverStarted.get(10, TimeUnit.SECONDS);
        
        objectMapper = new ObjectMapper();
        
        log.info("WebSocket test server started on port {}", TEST_PORT);
        
        // Give server a moment to fully initialize
        Thread.sleep(1000);
    }
    
    @AfterAll
    static void teardown() throws Exception {
        log.info("Tearing down WebSocket integration test");
        
        if (system != null) {
            httpServer.tell(new FxRateHttpServer.StopServer());
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
    
    @Test
    @Order(1)
    void testWebSocketConnection() throws Exception {
        log.info("Testing basic WebSocket connection");
        
        CountDownLatch connectionLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        
        WebSocketClient client = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("WebSocket connection opened");
                connectionLatch.countDown();
            }
            
            @Override
            public void onMessage(String message) {
                log.info("Received message: {}", message);
            }
            
            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed: {} - {}", code, reason);
                closeLatch.countDown();
            }
            
            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error", ex);
                error.set(ex);
                connectionLatch.countDown();
                closeLatch.countDown();
            }
        };
        
        try {
            client.connect();
            assertTrue(connectionLatch.await(10, TimeUnit.SECONDS), "Connection should be established");
            assertNull(error.get(), "No connection error should occur");
            
            client.close();
            assertTrue(closeLatch.await(5, TimeUnit.SECONDS), "Connection should close");
        } finally {
            if (client.isOpen()) {
                client.close();
            }
        }
    }
    
    @Test
    @Order(2)
    void testWebSocketFxRateCreation() throws Exception {
        log.info("Testing WebSocket FX rate creation functionality");
        
        CountDownLatch connectionLatch = new CountDownLatch(1);
        CountDownLatch messageLatch = new CountDownLatch(1);
        AtomicReference<String> receivedMessage = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        
        WebSocketClient client = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("WebSocket connection opened for FX rate creation test");
                connectionLatch.countDown();
            }
            
            @Override
            public void onMessage(String message) {
                log.info("Received FX rate creation message: {}", message);
                receivedMessage.set(message);
                messageLatch.countDown();
            }
            
            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed for FX rate creation test: {} - {}", code, reason);
            }
            
            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error in FX rate creation test", ex);
                error.set(ex);
                connectionLatch.countDown();
                messageLatch.countDown();
            }
        };
        
        try {
            client.connect();
            assertTrue(connectionLatch.await(10, TimeUnit.SECONDS), "Connection should be established");
            assertNull(error.get(), "No connection error should occur");
            
            // Send a command to create an FX rate
            String createCommand = "createFxRate:EUR_USD";
            client.send(createCommand);
            
            assertTrue(messageLatch.await(10, TimeUnit.SECONDS), "Should receive FX rate creation response");
            assertNull(error.get(), "No message error should occur");
            
            String response = receivedMessage.get();
            assertNotNull(response, "Should receive a response");
            
            // Parse the JSON response
            JsonNode json = objectMapper.readTree(response);
            assertEquals("fxRateCreated", json.get("type").asText());
            assertEquals("EUR_USD", json.get("pair").asText());
            assertTrue(json.has("timestamp"));
            
        } finally {
            if (client.isOpen()) {
                client.close();
            }
        }
    }
    
    @Test
    @Order(3)
    void testWebSocketHeartbeat() throws Exception {
        log.info("Testing WebSocket heartbeat functionality");
        
        CountDownLatch connectionLatch = new CountDownLatch(1);
        CountDownLatch heartbeatLatch = new CountDownLatch(1);
        AtomicReference<String> heartbeatMessage = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();
        
        WebSocketClient client = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("WebSocket connection opened for heartbeat test");
                connectionLatch.countDown();
            }
            
            @Override
            public void onMessage(String message) {
                log.info("Received heartbeat message: {}", message);
                try {
                    JsonNode json = objectMapper.readTree(message);
                    if ("heartbeat".equals(json.get("type").asText())) {
                        heartbeatMessage.set(message);
                        heartbeatLatch.countDown();
                    }
                } catch (Exception e) {
                    log.error("Error parsing message", e);
                }
            }
            
            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed for heartbeat test: {} - {}", code, reason);
            }
            
            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error in heartbeat test", ex);
                error.set(ex);
                connectionLatch.countDown();
                heartbeatLatch.countDown();
            }
        };
        
        try {
            client.connect();
            assertTrue(connectionLatch.await(10, TimeUnit.SECONDS), "Connection should be established");
            assertNull(error.get(), "No connection error should occur");
            
            // Wait for heartbeat (should come within 15 seconds based on our implementation)
            assertTrue(heartbeatLatch.await(15, TimeUnit.SECONDS), "Should receive heartbeat");
            assertNull(error.get(), "No heartbeat error should occur");
            
            String heartbeat = heartbeatMessage.get();
            assertNotNull(heartbeat, "Should receive heartbeat message");
            
            // Parse the JSON response
            JsonNode json = objectMapper.readTree(heartbeat);
            assertEquals("heartbeat", json.get("type").asText());
            assertTrue(json.has("timestamp"));
            
        } finally {
            if (client.isOpen()) {
                client.close();
            }
        }
    }
    
    @Test
    @Order(4)
    void testMultipleWebSocketConnections() throws Exception {
        log.info("Testing multiple WebSocket connections");
        
        int numClients = 3;
        CountDownLatch connectionLatch = new CountDownLatch(numClients);
        CountDownLatch messageLatch = new CountDownLatch(numClients);
        AtomicInteger messagesReceived = new AtomicInteger(0);
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
                    log.info("Client {} received: {}", clientId, message);
                    messagesReceived.incrementAndGet();
                    messageLatch.countDown();
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
                    messageLatch.countDown();
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
            
            // Send messages from each client
            for (int i = 0; i < numClients; i++) {
                clients[i].send("Message from client " + i);
            }
            
            assertTrue(messageLatch.await(10, TimeUnit.SECONDS), "All clients should receive responses");
            assertEquals(numClients, messagesReceived.get(), "Should receive responses from all clients");
            
        } finally {
            // Close all clients
            for (WebSocketClient client : clients) {
                if (client != null && client.isOpen()) {
                    client.close();
                }
            }
        }
    }
    
    @Test
    @Order(5)
    void testWebSocketConnectionDropHandling() throws Exception {
        log.info("Testing WebSocket connection drop handling");
        
        CountDownLatch connectionLatch = new CountDownLatch(1);
        CountDownLatch closeLatch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        
        WebSocketClient client = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("WebSocket connection opened for drop test");
                connectionLatch.countDown();
            }
            
            @Override
            public void onMessage(String message) {
                log.info("Received message in drop test: {}", message);
            }
            
            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed in drop test: {} - {}", code, reason);
                closeLatch.countDown();
            }
            
            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error in drop test", ex);
                error.set(ex);
                connectionLatch.countDown();
                closeLatch.countDown();
            }
        };
        
        try {
            client.connect();
            assertTrue(connectionLatch.await(10, TimeUnit.SECONDS), "Connection should be established");
            assertNull(error.get(), "No connection error should occur");
            
            // Abruptly close the connection
            client.closeBlocking();
            assertTrue(closeLatch.await(5, TimeUnit.SECONDS), "Connection should close gracefully");
            
        } finally {
            if (client.isOpen()) {
                client.close();
            }
        }
    }
}