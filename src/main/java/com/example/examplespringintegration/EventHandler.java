package com.example.examplespringintegration;

import com.example.examplespringintegration.kafka.ExampleMessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.example.examplespringintegration.EventProcessingResult.COMPLETED;

@Component
@Slf4j
@RequiredArgsConstructor
public class EventHandler {

    private final ExampleMessageProducer exampleMessageProducer;

    public CompletableFuture<EventProcessingResult> completed(String message, String traceId) {
        // traceId from Kafka Header with name "TRACE_ID" (optional)
        var start = System.currentTimeMillis();
        while (true) {
            var stop = System.currentTimeMillis();
            if (stop - start > 300) { // 300ms
                break;
            }
        }

        CompletableFuture<EventProcessingResult> result;

        if (start % 3 == 0) {
            result = exampleMessageProducer.sendAsyncMessage("example-notifications", "next-" + UUID.randomUUID()).thenApplyAsync(r -> COMPLETED);
        } else {
            result = EventProcessingResult.completed();
        }

        log.info(Application.INSTANCE_ID + " | Consumed: " + message);
        return result;
    }
}
