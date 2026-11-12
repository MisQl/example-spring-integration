package com.example.examplespringintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class EventHandler {

    public CompletableFuture<EventProcessingResult> completed(String message, String traceId) {
        // traceId from Kafka Header with name "TRACE_ID" (optional)
        log.info(Application.INSTANCE_ID + " | Consumed: " + message);
        return EventProcessingResult.completed();
    }
}
