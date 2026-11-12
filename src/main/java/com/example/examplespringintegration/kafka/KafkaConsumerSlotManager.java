package com.example.examplespringintegration.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.integration.jdbc.lock.JdbcLockRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.example.examplespringintegration.Application.INSTANCE_ID;

/**
 * Decides which Kafka topics this instance consumes by claiming an active slot from the KAFKA_CONSUMER_CONFIGURATION table.
 * <p>
 * A slot is claimed by holding the distributed lock "kafka-consumer-slot-N". While the lock is held the
 * {@code @KafkaListener}s of the slot's topics are running; the lock is renewed periodically. An instance that finds no free slot
 * stays on standby and retries, so it takes over when another instance stops or dies (its lock expires).
 * Only slots with ACTIVE = true are claimed; an instance whose slot is deactivated releases it at the next check
 * and claims another active slot if one is free.
 * <p>
 * KAFKA_CONSUMER_CONFIGURATION.INSTANCE_ID is optional configuration that this class only reads: a slot with an INSTANCE_ID
 * is reserved for the instance with that id, a slot without one can be claimed by any instance. An instance tries
 * the slots reserved for it first. If its slot is reserved for another instance later, it releases the slot.
 * Only slots with ACTIVE = true are claimed; an instance whose slot is deactivated releases it at the next check
 * and claims another active slot if one is free.
 * <p>
 * JDBC locks are bound to the thread that acquired them, so claim, renew and release all run on one thread.
 */
@Component
@Slf4j
public class KafkaConsumerSlotManager implements SmartLifecycle {

    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(10);

    private final JdbcTemplate jdbcTemplate;
    private final JdbcLockRegistry lockRegistry;
    private final KafkaListenerEndpointRegistry listenerRegistry;

    private ScheduledExecutorService executor;
    private volatile boolean running;

    // accessed only from the executor thread
    private int slotId;
    private String lockKey;
    private Lock lock;
    private List<String> topics = List.of();

    public KafkaConsumerSlotManager(JdbcTemplate jdbcTemplate, JdbcLockRegistry lockRegistry,
                                    KafkaListenerEndpointRegistry listenerRegistry) {
        this.jdbcTemplate = jdbcTemplate;
        this.lockRegistry = lockRegistry;
        this.listenerRegistry = listenerRegistry;
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
        var slots = jdbcTemplate.query("""
                SELECT SLOT, TOPICS FROM KAFKA_CONSUMER_CONFIGURATION
                WHERE ACTIVE AND (INSTANCE_ID IS NULL OR INSTANCE_ID = ?)
                ORDER BY CASE WHEN INSTANCE_ID IS NULL THEN 1 ELSE 0 END, SLOT
                """, (rs, rowNum) -> new Slot(rs.getInt("SLOT"), rs.getString("TOPICS")), INSTANCE_ID);
        for (var slot : slots) {
            var key = "kafka-consumer-slot-" + slot.id();
            var candidate = lockRegistry.obtain(key);
            if (candidate.tryLock()) {
                slotId = slot.id();
                lockKey = key;
                lock = candidate;
                topics = Arrays.stream(slot.topics().split(",")).map(String::trim).toList();
                startListeners();
                log.info("{} | Assigned topics to consume: {}", INSTANCE_ID, String.join(", ", topics));
                return;
            }
        }
        log.info("{} - no free kafka consumer slot ({} available to this instance), on standby", INSTANCE_ID, slots.size());
    }

    private void renew() {
        if (!isAvailable(slotId)) {
            // The slot was deactivated, deleted or reserved for another instance: give it up and look for another one.
            log.info("{} - kafka consumer slot {} is no longer available to this instance", INSTANCE_ID, slotId);
            release();
            claim();
            return;
        }
        try {
            lockRegistry.renewLock(lockKey);
        } catch (Exception e) {
            // The lock expired and may now belong to another instance: stop consuming and go back to claiming.
            log.warn("{} - lost {}, stopping kafka consumption", INSTANCE_ID, lockKey, e);
            stopListeners();
            lock = null;
            lockKey = null;
        }
    }

    /** Whether the slot is still active and not reserved for another instance. */
    private boolean isAvailable(int slot) {
        var count = jdbcTemplate.queryForObject("""
                SELECT COUNT(*) FROM KAFKA_CONSUMER_CONFIGURATION
                WHERE SLOT = ? AND ACTIVE AND (INSTANCE_ID IS NULL OR INSTANCE_ID = ?)
                """, Integer.class, slot, INSTANCE_ID);
        return count != null && count > 0;
    }

    private void release() {
        if (lock == null) {
            return;
        }
        try {
            stopListeners();
            lock.unlock();
            log.info("{} - released {}", INSTANCE_ID, lockKey);
        } catch (Exception e) {
            log.warn("{} - failed to release {}", INSTANCE_ID, lockKey, e);
        } finally {
            lock = null;
            lockKey = null;
        }
    }

    /** Starts the {@code @KafkaListener} containers of the given topics (listener id = topic name). */
    private void startListeners() {
        for (var topic : topics) {
            var container = listenerRegistry.getListenerContainer(topic);
            if (container == null) {
                log.error("{} - no @KafkaListener with id '{}', topic is not consumed", INSTANCE_ID, topic);
            } else {
                container.start();
            }
        }
    }

    private void stopListeners() {
        for (var topic : topics) {
            var container = listenerRegistry.getListenerContainer(topic);
            if (container != null) {
                container.stop();
            }
        }
        topics = List.of();
    }

    private record Slot(int id, String topics) {
    }
}
