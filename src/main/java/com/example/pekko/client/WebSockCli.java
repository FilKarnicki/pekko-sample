package com.example.pekko.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebSockCli {
    private static final Logger log = LoggerFactory.getLogger(WebSockCli.class);
    private static final int TEST_PORT = 8080; // Use different port for WebSocket testing
    private static final String WS_URL = "ws://localhost:" + TEST_PORT + "/ws";

    public static void main(String[] args) throws URISyntaxException, InterruptedException {
        new WebSockCli();
    }
    public WebSockCli() throws URISyntaxException, InterruptedException {
        WebSocketClient client = new WebSocketClient(new URI(WS_URL)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                log.info("WebSocket connection opened for FX rate creation test");
            }

            @Override
            public void onMessage(String message) {
                log.info("Received FX rate creation message: {}", message);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                log.info("WebSocket connection closed for FX rate creation test: {} - {}", code, reason);
            }

            @Override
            public void onError(Exception ex) {
                log.error("WebSocket error in FX rate creation test", ex);
            }
        };

        try {
            client.connect();
            Thread.sleep(100_000);
        }  finally {
            if (client.isOpen()) {
                client.close();
            }
        }
    }
}
