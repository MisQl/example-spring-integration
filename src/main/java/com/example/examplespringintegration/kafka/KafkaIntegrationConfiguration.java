package com.example.examplespringintegration.kafka;

import lombok.extern.slf4j.Slf4j;
import static com.example.examplespringintegration.Application.INSTANCE_ID;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.MessageChannel;

@Configuration
@EnableConfigurationProperties(KafkaProperties.class)
@Slf4j
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

    /** Consumes the topics configured for this instance (app.kafka.consume-topics) and logs every record. */
    @Bean
    IntegrationFlow kafkaInboundFlow(ConsumerFactory<?, ?> consumerFactory, KafkaProperties properties) {
        return IntegrationFlow
                .from(Kafka.messageDrivenChannelAdapter(consumerFactory, properties.consumeTopics().toArray(String[]::new)))
                .handle(message -> log.info("{} consumed topic={} partition={} offset={} key={} payload={}",
                        INSTANCE_ID,
                        message.getHeaders().get(KafkaHeaders.RECEIVED_TOPIC),
                        message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION),
                        message.getHeaders().get(KafkaHeaders.OFFSET),
                        message.getHeaders().get(KafkaHeaders.RECEIVED_KEY),
                        message.getPayload()))
                .get();
    }
}
