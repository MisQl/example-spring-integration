package com.example.examplespringintegration;

import com.example.examplespringintegration.kafka.ExampleMessageProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.integration.leader.event.OnRevokedEvent;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class ApplicationDispatcher {

    private final ExampleMessageProducer exampleMessageProducer;

    @EventListener
    public void onGranted(OnGrantedEvent grantedEvent) {
        log.info(Application.INSTANCE_ID + " - granted " + grantedEvent.getRole());
        exampleMessageProducer.sendExampleMessages();
    }

    @EventListener
    public void onRevoked(OnRevokedEvent revokedEvent) {
        log.info(Application.INSTANCE_ID + " - revoked " + revokedEvent.getRole());
    }
}
