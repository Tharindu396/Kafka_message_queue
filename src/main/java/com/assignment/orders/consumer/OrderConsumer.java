package com.assignment.orders.consumer;

import com.assignment.orders.avro.Order;
import com.assignment.orders.aggregation.PriceAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Consumes Avro orders, processes them and updates the running average.
 *
 * There is no try/catch here on purpose: exceptions propagate to the container's
 * {@code DefaultErrorHandler}, which owns retry and DLQ routing
 * (see {@code KafkaErrorHandlingConfig}). Keeping the failure policy out of the
 * listener is what lets it be changed in one place.
 */
@Component
@Profile("consumer")
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final OrderProcessor processor;
    private final PriceAggregator aggregator;

    public OrderConsumer(OrderProcessor processor, PriceAggregator aggregator) {
        this.processor = processor;
        this.aggregator = aggregator;
    }

    @KafkaListener(
            topics = "${app.topics.orders}",
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "1")
    public void onOrder(Order order,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset) {

        processor.process(order);                       // may throw -> retry / DLQ
        double average = aggregator.record(order);      // only reached on success

        log.info("CONSUME | orderId={} product={} price={} | p{}@{} | count={} RUNNING AVG={}",
                order.getOrderId(), order.getProduct(), String.format("%.2f", order.getPrice()),
                partition, offset, aggregator.getCount(), String.format("%.2f", average));
    }
}
