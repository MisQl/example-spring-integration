package com.example.examplespringintegration.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * @param topics           topics created on startup and written to by the example producer
 * @param partitions       number of partitions per topic
 * @param messagesPerTopic number of example messages sent to each topic on startup
 * @param processingMode   how the listener waits for the handler's future; defaults to {@link ProcessingMode#JOIN}
 */
@ConfigurationProperties("app.kafka")
public record KafkaProperties(List<String> topics, int partitions, int messagesPerTopic, ProcessingMode processingMode) {

    public KafkaProperties {
        if (processingMode == null) {
            processingMode = ProcessingMode.JOIN;
        }
    }

    public enum ProcessingMode {

        /** The consumer thread blocks until the handler's future completes, then acknowledges (in order, with retries). */
        JOIN,

        /** The consumer thread moves on at once; the record is acknowledged from a callback when the future completes. */
        WHEN_COMPLETE
    }
}
