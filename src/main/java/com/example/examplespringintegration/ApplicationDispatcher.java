package com.example.examplespringintegration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.integration.leader.event.OnGrantedEvent;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ApplicationDispatcher {

    @EventListener
    public void onGranted(OnGrantedEvent grantedEvent) {
        log.info(Application.INSTANCE_ID + " - granted " + grantedEvent.getRole());
    }

    @EventListener
    public void onRevoked(OnGrantedEvent grantedEvent) {
        log.info(Application.INSTANCE_ID + " - revoked " + grantedEvent.getRole());
    }
}
