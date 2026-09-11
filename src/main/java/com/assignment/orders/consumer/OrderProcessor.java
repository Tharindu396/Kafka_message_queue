package com.assignment.orders.consumer;

import com.assignment.orders.avro.Order;
import com.assignment.orders.exception.PermanentProcessingException;
import com.assignment.orders.exception.TransientProcessingException;
import com.assignment.orders.producer.OrderProducer;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The "business logic". In a real system this would write to a database or call a
 * downstream service; here it validates the order and simulates the two kinds of
 * failure the assignment asks us to handle.
 */
@Component
public class OrderProcessor {

    /** Delivery attempts per orderId, used to let FLAKY-ITEM succeed on the third try. */
    private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

    public void process(Order order) {

        // --- Permanent failure: the data itself is wrong, retrying cannot help. ---
        if (order.getPrice() <= 0f) {
            throw new PermanentProcessingException(
                    "invalid price " + order.getPrice() + " for order " + order.getOrderId());
        }

        String product = order.getProduct();

        // --- Transient failure that never clears: exhausts retries, lands in the DLQ. ---
        if (OrderProducer.ALWAYS_FAIL_PRODUCT.equals(product)) {
            throw new TransientProcessingException(
                    "pricing service unavailable for order " + order.getOrderId());
        }

        // --- Transient failure that clears: fails twice, succeeds on the third attempt. ---
        if (OrderProducer.FLAKY_PRODUCT.equals(product)) {
            int attempt = attempts.merge(order.getOrderId(), 1, Integer::sum);
            if (attempt < 3) {
                throw new TransientProcessingException(
                        "database timeout on order " + order.getOrderId() + " (attempt " + attempt + ")");
            }
            attempts.remove(order.getOrderId());
        }

        // Success path - the caller folds the order into the running aggregation.
    }
}
