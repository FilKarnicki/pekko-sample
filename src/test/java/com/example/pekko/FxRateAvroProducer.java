package com.example.pekko;

import com.example.pekko.model.FxRate;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class FxRateAvroProducer {
    private static final Logger log = LoggerFactory.getLogger(FxRateAvroProducer.class);

    private final Producer<String, FxRate> producer;
    private final String topicName;

    public FxRateAvroProducer(String bootstrapServers, String schemaRegistryUrl, String topicName) {
        this.topicName = topicName;
        this.producer = createProducer(bootstrapServers, schemaRegistryUrl);
    }

    private Producer<String, FxRate> createProducer(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // Schema Registry configuration
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);

        // Producer performance settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        return new KafkaProducer<>(props);
    }

    public CompletableFuture<RecordMetadata> sendFxRate(FxRate fxRate) {
        String key = fxRate.getFromCurrency() + "_" + fxRate.getToCurrency();
        ProducerRecord<String, FxRate> record = new ProducerRecord<>(topicName, key, fxRate);

        log.debug("Sending FxRate: {} {} -> {} at rate {}",
                fxRate.getId(), fxRate.getFromCurrency(), fxRate.getToCurrency(), fxRate.getRate());

        return CompletableFuture.supplyAsync(() -> {
                    try {
                        return producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Failed to send FxRate record", exception);
                            } else {
                                log.debug("Successfully sent FxRate to partition {} offset {}",
                                        metadata.partition(), metadata.offset());
                            }
                        }).get();
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    public CompletableFuture<Void> sendFxRates(List<FxRate> fxRates) {
        log.info("Sending {} FxRate records to topic {}", fxRates.size(), topicName);

        CompletableFuture<Void> allSent = new CompletableFuture<>();

        CompletableFuture<RecordMetadata>[] futures = fxRates.stream()
                .map(this::sendFxRate)
                .map(future -> {
                    CompletableFuture<RecordMetadata> cf = new CompletableFuture<>();
                    future.whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            cf.completeExceptionally(throwable);
                        } else {
                            cf.complete(result);
                        }
                    });
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        log.error("Failed to send some FxRate records", throwable);
                        allSent.completeExceptionally(throwable);
                    } else {
                        log.info("Successfully sent all {} FxRate records", fxRates.size());
                        allSent.complete(null);
                    }
                });

        return allSent;
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        try {
            producer.close();
            log.info("FxRateAvroProducer closed successfully");
        } catch (Exception e) {
            log.error("Error closing FxRateAvroProducer", e);
        }
    }
}