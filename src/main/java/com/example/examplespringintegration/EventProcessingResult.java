package com.example.examplespringintegration;

import java.util.concurrent.CompletableFuture;

public record EventProcessingResult() {

    public static final EventProcessingResult COMPLETED = new EventProcessingResult();

    public static CompletableFuture<EventProcessingResult> completed() {
        return CompletableFuture.completedFuture(COMPLETED);
    }
}
