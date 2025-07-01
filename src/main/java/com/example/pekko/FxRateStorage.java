package com.example.pekko;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import com.example.pekko.model.FxRate;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class FxRateStorage extends AbstractBehavior<FxRateStorage.Command> {
    private static final Logger log = LoggerFactory.getLogger(FxRateStorage.class);
    
    public interface Command {}
    
    public static final class StoreFxRate implements Command {
        public final FxRate fxRate;
        public final ActorRef<StoreResponse> replyTo;
        
        public StoreFxRate(FxRate fxRate, ActorRef<StoreResponse> replyTo) {
            this.fxRate = fxRate;
            this.replyTo = replyTo;
        }
    }
    
    public static final class GetFxRates implements Command {
        public final String currencyPair;
        public final ActorRef<GetResponse> replyTo;
        
        public GetFxRates(String currencyPair, ActorRef<GetResponse> replyTo) {
            this.currencyPair = currencyPair;
            this.replyTo = replyTo;
        }
    }
    
    public static final class GetAllFxRates implements Command {
        public final ActorRef<GetAllResponse> replyTo;
        
        public GetAllFxRates(ActorRef<GetAllResponse> replyTo) {
            this.replyTo = replyTo;
        }
    }
    
    public static final class Subscribe implements Command {
        public final ActorRef<FxRateUpdate> subscriber;
        
        public Subscribe(ActorRef<FxRateUpdate> subscriber) {
            this.subscriber = subscriber;
        }
    }
    
    public static final class Unsubscribe implements Command {
        public final ActorRef<FxRateUpdate> subscriber;
        
        public Unsubscribe(ActorRef<FxRateUpdate> subscriber) {
            this.subscriber = subscriber;
        }
    }
    
    public interface Response {}
    
    public static final class StoreResponse implements Response {
        public final boolean success;
        public final String error;
        
        public StoreResponse(boolean success, String error) {
            this.success = success;
            this.error = error;
        }
        
        public static StoreResponse success() {
            return new StoreResponse(true, null);
        }
        
        public static StoreResponse failure(String error) {
            return new StoreResponse(false, error);
        }
    }
    
    public static final class GetResponse implements Response {
        public final Set<String> fxRates;
        public final boolean success;
        
        public GetResponse(Set<String> fxRates, boolean success) {
            this.fxRates = fxRates;
            this.success = success;
        }
    }
    
    public static final class GetAllResponse implements Response {
        public final Map<String, Set<String>> allFxRates;
        public final boolean success;
        
        public GetAllResponse(Map<String, Set<String>> allFxRates, boolean success) {
            this.allFxRates = allFxRates;
            this.success = success;
        }
    }
    
    public static final class FxRateUpdate implements Response {
        public final FxRate fxRate;
        public final String currencyPair;
        
        public FxRateUpdate(FxRate fxRate, String currencyPair) {
            this.fxRate = fxRate;
            this.currencyPair = currencyPair;
        }
    }
    
    private final ConcurrentHashMap<String, Set<String>> storage;
    private final ObjectMapper objectMapper;
    private final Set<ActorRef<FxRateUpdate>> subscribers;
    
    public static Behavior<Command> create() {
        return Behaviors.setup(FxRateStorage::new);
    }
    
    private FxRateStorage(ActorContext<Command> context) {
        super(context);
        this.storage = new ConcurrentHashMap<>();
        this.objectMapper = new ObjectMapper();
        this.subscribers = ConcurrentHashMap.newKeySet();
        
        log.info("FxRateStorage actor started with in-memory storage and subscription support");
    }
    
    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StoreFxRate.class, this::storeFxRate)
                .onMessage(GetFxRates.class, this::getFxRates)
                .onMessage(GetAllFxRates.class, this::getAllFxRates)
                .onMessage(Subscribe.class, this::subscribe)
                .onMessage(Unsubscribe.class, this::unsubscribe)
                .build();
    }
    
    private Behavior<Command> storeFxRate(StoreFxRate command) {
        try {
            String currencyPair = command.fxRate.getFromCurrency() + "_" + command.fxRate.getToCurrency();
            String fxRateJson = objectMapper.writeValueAsString(convertFxRateToJson(command.fxRate));
            
            storage.computeIfAbsent(currencyPair, k -> ConcurrentHashMap.newKeySet()).add(fxRateJson);
            
            log.debug("Stored FxRate: {} = {}", currencyPair, fxRateJson);
            
            // Notify all subscribers about the new FX rate
            FxRateUpdate update = new FxRateUpdate(command.fxRate, currencyPair);
            subscribers.forEach(subscriber -> {
                try {
                    subscriber.tell(update);
                    log.debug("Notified subscriber about FX rate update: {}", currencyPair);
                } catch (Exception e) {
                    log.warn("Failed to notify subscriber about FX rate update", e);
                    // Remove dead subscribers
                    subscribers.remove(subscriber);
                }
            });
            
            command.replyTo.tell(StoreResponse.success());
            
        } catch (Exception e) {
            log.error("Failed to store FxRate", e);
            command.replyTo.tell(StoreResponse.failure(e.getMessage()));
        }
        
        return this;
    }
    
    private Behavior<Command> getFxRates(GetFxRates command) {
        Set<String> fxRates = storage.getOrDefault(command.currencyPair, new HashSet<>());
        command.replyTo.tell(new GetResponse(new HashSet<>(fxRates), true));
        return this;
    }
    
    private Behavior<Command> getAllFxRates(GetAllFxRates command) {
        Map<String, Set<String>> allRates = new HashMap<>();
        storage.forEach((key, value) -> allRates.put(key, new HashSet<>(value)));
        command.replyTo.tell(new GetAllResponse(allRates, true));
        return this;
    }
    
    private Behavior<Command> subscribe(Subscribe command) {
        subscribers.add(command.subscriber);
        log.info("Added WebSocket subscriber. Total subscribers: {}", subscribers.size());
        return this;
    }
    
    private Behavior<Command> unsubscribe(Unsubscribe command) {
        subscribers.remove(command.subscriber);
        log.info("Removed WebSocket subscriber. Total subscribers: {}", subscribers.size());
        return this;
    }
    
    private Object convertFxRateToJson(FxRate fxRate) {
        return new Object() {
            public final String id = fxRate.getId().toString();
            public final String fromCurrency = fxRate.getFromCurrency().toString();
            public final String toCurrency = fxRate.getToCurrency().toString();
            public final double rate = fxRate.getRate();
            public final long timestamp = fxRate.getTimestamp();
            public final String source = fxRate.getSource() != null ? fxRate.getSource().toString() : null;
        };
    }
}