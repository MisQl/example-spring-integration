package com.example.examplespringintegration.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @param topics           topics created on startup and written to by the example producer
 * @param consumeTopics    topics this instance consumes; defaults to all {@code topics}
 * @param partitions       number of partitions per topic
 * @param messagesPerTopic number of example messages sent to each topic on startup
 */
@ConfigurationProperties("app.kafka")
public record KafkaProperties(List<String> topics, List<String> consumeTopics, int partitions, int messagesPerTopic) {

    public KafkaProperties {
        if (consumeTopics == null || consumeTopics.isEmpty()) {
            consumeTopics = topics;
        }
    }
}
