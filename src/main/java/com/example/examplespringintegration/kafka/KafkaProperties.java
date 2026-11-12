package com.example.examplespringintegration.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("app.kafka")
public record KafkaProperties(List<String> topics, int messagesPerTopic) {
}
