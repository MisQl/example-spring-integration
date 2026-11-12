package com.example.examplespringintegration.kafka;

import com.example.examplespringintegration.EventHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.example.examplespringintegration.Application.INSTANCE_ID;

/**
 * Decides which Kafka topics this instance consumes by claiming a slot from the KAFKA_CONSUMER_SLOT table.
 * <p>
 * A slot is claimed by holding the distributed lock "kafka-consumer-slot-N". While the lock is held the inbound
 * flow for the slot's topics is registered; the lock is renewed periodically. An instance that finds no free slot
 * stays on standby and retries, so it takes over when another instance stops or dies (its lock expires).
 * <p>
 * JDBC locks are bound to the thread that acquired them, so claim, renew and release all run on one thread.
 */
@Component
@Slf4j
public class KafkaConsumerSlotManager implements SmartLifecycle {

    public static final String TRACE_ID_HEADER = "TRACE_ID";

    private static final String FLOW_ID = "kafkaInboundFlow";
    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(10);

    private final JdbcTemplate jdbcTemplate;
    private final JdbcLockRegistry lockRegistry;
    private final IntegrationFlowContext flowContext;
    private final ConsumerFactory<?, ?> consumerFactory;
    private final EventHandler eventHandler;

    private ScheduledExecutorService executor;
    private volatile boolean running;

    // accessed only from the executor thread
    private String lockKey;
    private Lock lock;

    public KafkaConsumerSlotManager(JdbcTemplate jdbcTemplate, JdbcLockRegistry lockRegistry,
                                    IntegrationFlowContext flowContext, ConsumerFactory<?, ?> consumerFactory,
                                    EventHandler eventHandler) {
        this.jdbcTemplate = jdbcTemplate;
        this.lockRegistry = lockRegistry;
        this.flowContext = flowContext;
        this.consumerFactory = consumerFactory;
        this.eventHandler = eventHandler;
    }

    @Override
    public void start() {
        running = true;
        executor = Executors.newSingleThreadScheduledExecutor(runnable -> new Thread(runnable, "kafka-consumer-slot"));
        executor.scheduleWithFixedDelay(this::claimOrRenew, 0, CHECK_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        running = false;
        executor.execute(this::release);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void claimOrRenew() {
        try {
            if (lock == null) {
                claim();
            } else {
                renew();
            }
        } catch (Exception e) {
            log.error("{} - kafka consumer slot check failed", INSTANCE_ID, e);
        }
    }

    private void claim() {
        List<Slot> slots = jdbcTemplate.query("SELECT SLOT, TOPICS FROM KAFKA_CONSUMER_SLOT ORDER BY SLOT",
                (rs, rowNum) -> new Slot(rs.getInt("SLOT"), rs.getString("TOPICS")));
        for (Slot slot : slots) {
            String key = "kafka-consumer-slot-" + slot.id();
            Lock candidate = lockRegistry.obtain(key);
            if (candidate.tryLock()) {
                lockKey = key;
                lock = candidate;
                String[] topics = Arrays.stream(slot.topics().split(",")).map(String::trim).toArray(String[]::new);
                flowContext.registration(inboundFlow(topics)).id(FLOW_ID).register();
                log.info("{} | Assigned topics to consume: {}", INSTANCE_ID, String.join(", ", topics));
                return;
            }
        }
        log.info("{} - no free kafka consumer slot ({} defined), on standby", INSTANCE_ID, slots.size());
    }

    private void renew() {
        try {
            lockRegistry.renewLock(lockKey);
        } catch (Exception e) {
            // The lock expired and may now belong to another instance: stop consuming and go back to claiming.
            log.warn("{} - lost {}, stopping kafka consumption", INSTANCE_ID, lockKey, e);
            flowContext.remove(FLOW_ID);
            lock = null;
            lockKey = null;
        }
    }

    private void release() {
        if (lock == null) {
            return;
        }
        try {
            flowContext.remove(FLOW_ID);
            lock.unlock();
            log.info("{} - released {}", INSTANCE_ID, lockKey);
        } catch (Exception e) {
            log.warn("{} - failed to release {}", INSTANCE_ID, lockKey, e);
        } finally {
            lock = null;
            lockKey = null;
        }
    }

    /** Consumes the given topics and passes every record to the {@link EventHandler}. */
    private IntegrationFlow inboundFlow(String[] topics) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(consumerFactory, topics)
                        .configureListenerContainer(container -> container.ackMode(ContainerProperties.AckMode.MANUAL)))
                .handle(this::handle)
                .get();
    }

    /**
     * Runs on the Kafka consumer thread and waits for the handler's future. The record is acknowledged manually
     * only after processing has finished; a failed future is rethrown without acknowledging and handled by the
     * listener container (retry, then skip).
     */
    private void handle(Message<?> message) {
        String traceId = traceId(message);
        log.debug("{} - handling topic={} partition={} offset={} traceId={}", INSTANCE_ID,
                message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION),
                message.getHeaders().get(KafkaHeaders.OFFSET),
                traceId);
        eventHandler.completed(String.valueOf(message.getPayload()), traceId).join();
        var acknowledgment = message.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
        if (acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    /** The optional TRACE_ID Kafka header; raw bytes when the producer did not send Spring's header type info. */
    private static String traceId(Message<?> message) {
        Object traceId = message.getHeaders().get(TRACE_ID_HEADER);
        if (traceId instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return traceId != null ? traceId.toString() : null;
    }

    private record Slot(int id, String topics) {
    }
}
