package com.example.examplespringintegration.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaIntegrationConfiguration {

    /** Topics are created on startup by the auto-configured KafkaAdmin. */
    @Bean
    KafkaAdmin.NewTopics exampleTopics(KafkaProperties properties) {
        return new KafkaAdmin.NewTopics(properties.topics().stream()
                .map(topic -> TopicBuilder.name(topic).partitions(properties.partitions()).replicas(1).build())
                .toArray(NewTopic[]::new));
    }
}
