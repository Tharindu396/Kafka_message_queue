package com.assignment.orders.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Creates the two topics on startup so the demo never depends on auto-creation.
 * The DLQ deliberately has the same partition count as the source topic, which
 * lets the DLQ publisher preserve the original partition of a failed record.
 */
@Configuration
public class TopicConfig {

    @Bean
    NewTopic ordersTopic(@Value("${app.topics.orders}") String name,
                         @Value("${app.topics.partitions}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }

    @Bean
    NewTopic dlqTopic(@Value("${app.topics.dlq}") String name,
                      @Value("${app.topics.partitions}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }
}
