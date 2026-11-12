package com.example.examplespringintegration.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static com.example.examplespringintegration.Application.INSTANCE_ID;
import static com.example.examplespringintegration.kafka.KafkaIntegrationConfiguration.KAFKA_OUTBOUND_CHANNEL;

/** Sends the example messages to every configured topic once the application has started. */
@Component
@Slf4j
public class ExampleMessageProducer implements ApplicationRunner {

    private final MessageChannel kafkaOutboundChannel;
    private final KafkaProperties properties;

    public ExampleMessageProducer(@Qualifier(KAFKA_OUTBOUND_CHANNEL) MessageChannel kafkaOutboundChannel,
                                  KafkaProperties properties) {
        this.kafkaOutboundChannel = kafkaOutboundChannel;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        for (String topic : properties.topics()) {
            for (int i = 1; i <= properties.messagesPerTopic(); i++) {
                String payload = topic + " message " + i + " from " + INSTANCE_ID;
                kafkaOutboundChannel.send(MessageBuilder.withPayload(payload)
                        .setHeader(KafkaHeaders.TOPIC, topic)
                        .setHeader(KafkaHeaders.KEY, topic + "-" + i)
                        .setHeader(KafkaConsumerSlotManager.TRACE_ID_HEADER, UUID.randomUUID().toString())
                        .build());
                log.info("Sent topic={} payload={}", topic, payload);
            }
        }
    }
}
