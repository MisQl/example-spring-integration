package com.example.examplespringintegration.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.example.examplespringintegration.Application.INSTANCE_ID;

/** Sends the example messages to every configured topic; called by the ApplicationDispatcher on the leader. */
@Component
@Slf4j
public class ExampleMessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaProperties properties;

    public ExampleMessageProducer(KafkaTemplate<String, String> kafkaTemplate, KafkaProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void sendExampleMessages() {
        for (String topic : properties.topics()) {
            for (int i = 1; i <= properties.messagesPerTopic(); i++) {
                String payload = topic + " message " + i + " from " + INSTANCE_ID;
                var record = new ProducerRecord<>(topic, topic + "-" + i, payload);
                record.headers().add(KafkaEventListener.TRACE_ID_HEADER,
                        UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                kafkaTemplate.send(record).whenComplete((result, failure) -> {
                    if (failure != null) {
                        log.error("Failed to send topic={} payload={}", topic, payload, failure);
                    }
                });
                log.info("Sent topic={} payload={}", topic, payload);
            }
        }
    }

    public CompletableFuture<SendResult<String, String>> sendAsyncMessage(String topic, String message) {
        return kafkaTemplate.send(topic, message);
    }
}
