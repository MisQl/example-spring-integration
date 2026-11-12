package com.example.examplespringintegration.kafka;

import com.example.examplespringintegration.EventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
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
     * Runs on the Kafka consumer thread and waits for the handler's future. The record is acknowledged manually
     * only after processing has finished; a failed future is rethrown without acknowledging and handled by the
     * listener container (retry, then skip).
     */
    private void handle(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        String traceId = traceId(record);
        log.debug("{} - handling topic={} partition={} offset={} traceId={}", INSTANCE_ID, record.topic(), record.partition(), record.offset(), traceId);
        eventHandler.completed(record.value(), traceId).join();
        acknowledgment.acknowledge();
    }

    /** The optional TRACE_ID Kafka header, or null when the record has none. */
    private static String traceId(ConsumerRecord<String, String> record) {
        Header header = record.headers().lastHeader(TRACE_ID_HEADER);
        return header != null && header.value() != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
}
