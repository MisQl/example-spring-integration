package com.example.examplespringintegration.kafka;

import com.example.examplespringintegration.EventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

import static com.example.examplespringintegration.Application.INSTANCE_ID;

/**
 * One listener per topic, all passing records to the {@link EventHandler}.
 * <p>
 * The listeners do not start with the application: the {@link KafkaConsumerSlotManager} starts the ones whose id
 * matches a topic of the slot this instance has claimed. The listener id must therefore equal the topic name.
 * The number of consumer threads is configured per topic under {@code app.kafka.concurrency}.
 * {@code idIsGroup = false} keeps every listener in the shared consumer group (spring.kafka.consumer.group-id).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class KafkaEventListener {

    public static final String TRACE_ID_HEADER = "TRACE_ID";

    private final EventHandler eventHandler;
    private final KafkaProperties properties;

    @KafkaListener(id = "example-orders", topics = "example-orders", idIsGroup = false, autoStartup = "false", concurrency = "${app.kafka.concurrency.example-orders}")
    public void onOrder(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        handle(record, acknowledgment);
    }

    @KafkaListener(id = "example-payments", topics = "example-payments", idIsGroup = false, autoStartup = "false", concurrency = "${app.kafka.concurrency.example-payments}")
    public void onPayment(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        handle(record, acknowledgment);
    }

    @KafkaListener(id = "example-notifications", topics = "example-notifications", idIsGroup = false, autoStartup = "false", concurrency = "${app.kafka.concurrency.example-notifications}")
    public void onNotification(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        handle(record, acknowledgment);
    }

    /**
     * Passes the record to the {@link EventHandler} and acknowledges it according to app.kafka.processing-mode.
     * <ul>
     * <li>join: waits for the handler's future on the consumer thread and then acknowledges; a failed future is
     * rethrown without acknowledging and handled by the listener container (retry, then skip).</li>
     * <li>when-complete: returns at once and acknowledges from a callback when the future completes. A failed future
     * is logged and acknowledged, i.e. skipped without retry, so it cannot block later commits. Use it together with
     * spring.kafka.listener.async-acks=true; otherwise an acknowledgment can commit past records still in progress.</li>
     * </ul>
     */
    private void handle(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        var traceId = traceId(record);
        log.debug("{} - handling topic={} partition={} offset={} traceId={}", INSTANCE_ID, record.topic(), record.partition(), record.offset(), traceId);
        var result = eventHandler.completed(record.value(), traceId);
        switch (properties.processingMode()) {
            case JOIN -> {
                result.join();
                acknowledgment.acknowledge();
            }
            case WHEN_COMPLETE -> result.whenComplete((ignored, failure) -> {
                if (failure != null) {
                    log.error("{} - failed, skipping topic={} partition={} offset={} traceId={}", INSTANCE_ID, record.topic(), record.partition(), record.offset(), traceId, failure);
                }
                acknowledgment.acknowledge();
            });
        }
    }

    /** The optional TRACE_ID Kafka header, or null when the record has none. */
    private static String traceId(ConsumerRecord<String, String> record) {
        var header = record.headers().lastHeader(TRACE_ID_HEADER);
        return header != null && header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
}
