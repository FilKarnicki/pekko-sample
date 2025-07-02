package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import com.example.pekko.model.FxRate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.Response;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FxRateHttpServerIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(FxRateHttpServerIntegrationTest.class);
    
    private static ActorSystem<Void> system;
    private static ActorRef<FxRateStorage.Command> fxRateStorage;
    private static ActorRef<FxRateHttpServer.Command> httpServer;
    private static ServerBinding binding;
    private static AsyncHttpClient httpClient;
    private static ObjectMapper objectMapper;
    private static final int TEST_PORT = 18080; // Use different port for testing
    private static final String BASE_URL = "http://localhost:" + TEST_PORT;

    @BeforeAll
    static void setup() throws Exception {
        log.info("Setting up FxRateHttpServer integration test");
        
        // Create actor system with test configuration
        com.typesafe.config.Config testConfig = com.typesafe.config.ConfigFactory.load("application-test");
        system = ActorSystem.create(Behaviors.empty(), "FxRateHttpServerTest", testConfig);
        
        // Create storage actor
        fxRateStorage = system.systemActorOf(
                FxRateStorage.create(), 
                "fx-rate-storage-test", 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Create HTTP server actor
        httpServer = system.systemActorOf(
                FxRateHttpServer.create(fxRateStorage), 
                "fx-rate-http-server-test", 
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        // Start server and wait for it to be ready
        CompletableFuture<ServerBinding> bindingFuture = new CompletableFuture<>();
        
        ActorRef<FxRateHttpServer.ServerStarted> replyTo = system.systemActorOf(
                Behaviors.receive((context, message) -> {
                    bindingFuture.complete(message.binding);
                    return Behaviors.stopped();
                }),
                "server-started-reply",
                org.apache.pekko.actor.typed.Props.empty()
        );
        
        httpServer.tell(new FxRateHttpServer.StartServer("localhost", TEST_PORT, replyTo));
        binding = bindingFuture.get(10, TimeUnit.SECONDS);
        
        httpClient = new DefaultAsyncHttpClient();
        objectMapper = new ObjectMapper();
        
        log.info("Test server started on port {}", TEST_PORT);
        
        // Add some test data
        addTestData();
    }
    
    @AfterAll
    static void teardown() throws Exception {
        log.info("Tearing down FxRateHttpServer integration test");
        
        if (httpClient != null) {
            httpClient.close();
        }
        
        if (binding != null) {
            binding.unbind().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        
        if (system != null) {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
    
    private static void addTestData() throws Exception {
        log.info("Adding test data to storage");
        
        // Create some test FX rates
        FxRate usdEur = FxRate.newBuilder()
                .setId("test-1")
                .setFromCurrency("USD")
                .setToCurrency("EUR")
                .setRate(0.85)
                .setTimestamp(System.currentTimeMillis())
                .setSource("TEST")
                .build();
                
        FxRate gbpUsd = FxRate.newBuilder()
                .setId("test-2")
                .setFromCurrency("GBP")
                .setToCurrency("USD")
                .setRate(1.25)
                .setTimestamp(System.currentTimeMillis())
                .setSource("TEST")
                .build();
        
        // Store test data
        CompletionStage<FxRateStorage.StoreResponse> future1 = org.apache.pekko.actor.typed.javadsl.AskPattern
                .<FxRateStorage.Command, FxRateStorage.StoreResponse>ask(
                        fxRateStorage,
                        replyTo -> new FxRateStorage.StoreFxRate(usdEur, replyTo),
                        Duration.ofSeconds(5),
                        system.scheduler()
                );
                
        CompletionStage<FxRateStorage.StoreResponse> future2 = org.apache.pekko.actor.typed.javadsl.AskPattern
                .<FxRateStorage.Command, FxRateStorage.StoreResponse>ask(
                        fxRateStorage,
                        replyTo -> new FxRateStorage.StoreFxRate(gbpUsd, replyTo),
                        Duration.ofSeconds(5),
                        system.scheduler()
                );
        
        FxRateStorage.StoreResponse response1 = future1.toCompletableFuture().get(5, TimeUnit.SECONDS);
        FxRateStorage.StoreResponse response2 = future2.toCompletableFuture().get(5, TimeUnit.SECONDS);
        
        assertTrue(response1.success, "Failed to store test data 1");
        assertTrue(response2.success, "Failed to store test data 2");
        
        log.info("Test data added successfully");
    }
    
    @Test
    @Order(1)
    void testHealthEndpoint() throws Exception {
        log.info("Testing health endpoint");
        
        Response response = httpClient.prepareGet(BASE_URL + "/health")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(200, response.getStatusCode());
        assertEquals("FxRate service is healthy", response.getResponseBody());
    }
    
    @Test
    @Order(2)
    void testRootEndpoint() throws Exception {
        log.info("Testing root endpoint");
        
        Response response = httpClient.prepareGet(BASE_URL + "/")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(200, response.getStatusCode());
        String body = response.getResponseBody();
        assertTrue(body.contains("REST API: /api/fxrates"));
        assertTrue(body.contains("Health: /health"));
    }
    
    @Test
    @Order(3)
    void testGetAllFxRates() throws Exception {
        log.info("Testing get all FX rates endpoint");
        
        Response response = httpClient.prepareGet(BASE_URL + "/api/fxrates")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(200, response.getStatusCode());
        
        String body = response.getResponseBody();
        JsonNode json = objectMapper.readTree(body);
        
        assertNotNull(json);
        assertTrue(json.isObject());
        
        // Should contain our test currency pairs
        assertTrue(json.has("USD_EUR") || json.has("GBP_USD"), "Response should contain test currency pairs");
        
        log.info("Get all FX rates response: {}", body);
    }
    
    @Test
    @Order(4)
    void testGetFxRatesByPair() throws Exception {
        log.info("Testing get FX rates by pair endpoint");
        
        Response response = httpClient.prepareGet(BASE_URL + "/api/fxrates?pair=USD_EUR")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(200, response.getStatusCode());
        
        String body = response.getResponseBody();
        JsonNode json = objectMapper.readTree(body);
        
        assertNotNull(json);
        assertTrue(json.isArray());
        
        log.info("Get FX rates by pair response: {}", body);
    }
    
    @Test
    @Order(5)
    void testGetFxRatesForNonExistentPair() throws Exception {
        log.info("Testing get FX rates for non-existent pair");
        
        Response response = httpClient.prepareGet(BASE_URL + "/api/fxrates?pair=XXX_YYY")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(200, response.getStatusCode());
        
        String body = response.getResponseBody();
        JsonNode json = objectMapper.readTree(body);
        
        assertNotNull(json);
        assertTrue(json.isArray());
        assertEquals(0, json.size(), "Should return empty array for non-existent pair");
    }
    
    @Test
    @Order(6)
    void testInvalidEndpoint() throws Exception {
        log.info("Testing invalid endpoint");
        
        Response response = httpClient.prepareGet(BASE_URL + "/invalid/endpoint")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(404, response.getStatusCode());
    }
    
    @Test
    @Order(7)
    void testConcurrentRequests() throws Exception {
        log.info("Testing concurrent requests");
        
        int numRequests = 10;
        CompletableFuture<Response>[] futures = new CompletableFuture[numRequests];
        
        // Send multiple concurrent requests
        for (int i = 0; i < numRequests; i++) {
            futures[i] = httpClient.prepareGet(BASE_URL + "/api/fxrates")
                    .execute()
                    .toCompletableFuture();
        }
        
        // Wait for all responses
        for (int i = 0; i < numRequests; i++) {
            Response response = futures[i].get(10, TimeUnit.SECONDS);
            assertEquals(200, response.getStatusCode(), "Request " + i + " should succeed");
        }
        
        log.info("All {} concurrent requests completed successfully", numRequests);
    }
    
    @Test
    @Order(8)
    void testMethodNotAllowed() throws Exception {
        log.info("Testing method not allowed");
        
        Response response = httpClient.preparePost(BASE_URL + "/api/fxrates")
                .setBody("{\"test\": \"data\"}")
                .execute()
                .get(5, TimeUnit.SECONDS);
        
        assertEquals(405, response.getStatusCode());
    }
}