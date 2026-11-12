package com.example.examplespringintegration.kafka;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageChannel;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaIntegrationConfiguration {

    public static final String KAFKA_OUTBOUND_CHANNEL = "kafkaOutboundChannel";

    /** Topics are created on startup by the auto-configured KafkaAdmin. */
    @Bean
    KafkaAdmin.NewTopics exampleTopics(KafkaProperties properties) {
        return new KafkaAdmin.NewTopics(properties.topics().stream()
                .map(topic -> TopicBuilder.name(topic).partitions(properties.partitions()).replicas(1).build())
                .toArray(org.apache.kafka.clients.admin.NewTopic[]::new));
    }

    @Bean(KAFKA_OUTBOUND_CHANNEL)
    MessageChannel kafkaOutboundChannel() {
        return new DirectChannel();
    }

    /** Messages sent to kafkaOutboundChannel are published to the topic given in the kafka_topic header. */
    @Bean
    IntegrationFlow kafkaOutboundFlow(KafkaTemplate<?, ?> kafkaTemplate) {
        return IntegrationFlow.from(KAFKA_OUTBOUND_CHANNEL)
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate))
                .get();
    }
}
