package com.example.examplespringintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Optional;
import java.util.UUID;

@SpringBootApplication
@Slf4j
public class Application {

    /**
     * Identifies this instance (lock owner, KAFKA_CONSUMER_CONFIGURATION.INSTANCE_ID). Taken from the INSTANCE_ID environment
     * variable when set, so a slot can be reserved for the instance; otherwise random. At most 36 characters, and it
     * must be unique among running instances.
     */
    public final static String INSTANCE_ID = Optional.ofNullable(System.getenv("INSTANCE_ID"))
            .filter(id -> !id.isBlank())
            .orElseGet(() -> UUID.randomUUID().toString());

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
        log.info("Application started: " + INSTANCE_ID);
    }
}
