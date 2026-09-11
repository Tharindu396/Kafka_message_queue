package com.assignment.orders.producer;

import com.assignment.orders.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Publishes Avro-serialised {@link Order} messages to the "orders" topic.
 *
 * The schema is registered with the Confluent Schema Registry on first send, and every
 * message carries a 5-byte prefix (magic byte + schema id) instead of the schema itself.
 *
 * To make retry / DLQ behaviour demonstrable, a deterministic slice of the traffic is
 * deliberately poisoned - see {@code buildOrder}.
 */
@Component
@Profile("producer")
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private static final List<String> PRODUCTS = List.of("Item1", "Item2", "Item3", "Item4", "Item5");

    /** Consumer always fails these transiently -> retries exhausted -> DLQ. */
    public static final String ALWAYS_FAIL_PRODUCT = "ALWAYS-FAIL";
    /** Consumer fails these twice then succeeds -> demonstrates a successful retry. */
    public static final String FLAKY_PRODUCT = "FLAKY-ITEM";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String topic;
    private final AtomicLong sequence = new AtomicLong(1000);

    public OrderProducer(KafkaTemplate<String, Object> kafkaTemplate,
                         @Value("${app.topics.orders}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    @Scheduled(fixedDelayString = "${app.producer.interval-ms}")
    public void produce() {
        long n = sequence.incrementAndGet();
        Order order = buildOrder(n);

        // Keying by orderId guarantees per-order ordering and an even spread across partitions.
        kafkaTemplate.send(topic, order.getOrderId(), order)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("PRODUCE | failed to send {} : {}", order.getOrderId(), ex.getMessage());
                    } else {
                        log.info("PRODUCE | orderId={} product={} price={} -> {}-{}@{}",
                                order.getOrderId(), order.getProduct(),
                                String.format("%.2f", order.getPrice()),
                                topic,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    private Order buildOrder(long n) {
        String orderId = String.valueOf(n);
        float price = round2(ThreadLocalRandom.current().nextFloat() * 490f + 10f);
        String product = PRODUCTS.get(ThreadLocalRandom.current().nextInt(PRODUCTS.size()));

        // Deliberate failure injection so the live demo shows every path.
        if (n % 11 == 0) {
            product = ALWAYS_FAIL_PRODUCT;              // transient, never recovers -> DLQ
        } else if (n % 7 == 0) {
            price = -1f;                                // invalid data -> permanent -> DLQ
        } else if (n % 5 == 0) {
            product = FLAKY_PRODUCT;                    // transient, recovers on retry
        }

        return Order.newBuilder()
                .setOrderId(orderId)
                .setProduct(product)
                .setPrice(price)
                .build();
    }

    private static float round2(float value) {
        return Math.round(value * 100f) / 100f;
    }
}
