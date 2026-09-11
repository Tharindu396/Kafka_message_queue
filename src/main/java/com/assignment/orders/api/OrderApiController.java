package com.assignment.orders.api;

import com.assignment.orders.aggregation.PriceAggregator;
import com.assignment.orders.avro.Order;
import com.assignment.orders.dlq.DlqStore;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * HTTP front door for manual testing (Insomnia, Postman, curl).
 *
 * Kafka does not speak HTTP, so these endpoints exist purely so a request can be sent by
 * hand and its effect observed. The JSON body is converted to an Avro {@link Order} and
 * published through the same producer the scheduled producer uses - so a manual request
 * exercises the real code path, not a test-only shortcut.
 */
@RestController
@RequestMapping("/api")
public class OrderApiController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final PriceAggregator aggregator;
    private final DlqStore dlqStore;
    private final String ordersTopic;
    private final String dlqTopic;

    public OrderApiController(KafkaTemplate<String, Object> kafkaTemplate,
                              PriceAggregator aggregator,
                              DlqStore dlqStore,
                              @Value("${app.topics.orders}") String ordersTopic,
                              @Value("${app.topics.dlq}") String dlqTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.aggregator = aggregator;
        this.dlqStore = dlqStore;
        this.ordersTopic = ordersTopic;
        this.dlqTopic = dlqTopic;
    }

    /** Request body for POST /api/orders. Price is intentionally NOT validated as positive - sending a negative price is how the permanent-failure path is demonstrated. */
    public record OrderRequest(
            @NotBlank(message = "orderId is required") String orderId,
            @NotBlank(message = "product is required") String product,
            @NotNull(message = "price is required") Float price) {
    }

    /**
     * Publishes one Avro order and reports exactly where it landed.
     * Returns 201 with the partition and offset, so the response itself is evidence
     * that the message reached the broker.
     */
    @PostMapping("/orders")
    public ResponseEntity<Map<String, Object>> publish(@Valid @RequestBody OrderRequest request)
            throws Exception {

        Order order = Order.newBuilder()
                .setOrderId(request.orderId())
                .setProduct(request.product())
                .setPrice(request.price())
                .build();

        SendResult<String, Object> result = kafkaTemplate
                .send(ordersTopic, order.getOrderId(), order)
                .get(10, TimeUnit.SECONDS);

        RecordMetadata metadata = result.getRecordMetadata();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "published");
        body.put("topic", metadata.topic());
        body.put("partition", metadata.partition());
        body.put("offset", metadata.offset());
        body.put("order", Map.of(
                "orderId", order.getOrderId(),
                "product", order.getProduct(),
                "price", order.getPrice()));
        return ResponseEntity.status(HttpStatus.CREATED).body(body);
    }

    /** Live snapshot of the running aggregation - the same numbers the console prints. */
    @GetMapping("/stats")
    public Map<String, Object> stats() {
        return aggregator.snapshot();
    }

    /**
     * The dead letters seen so far, newest first, each with the headers explaining why it
     * failed. Requires the 'dlq' profile to be active (it is by default).
     */
    @GetMapping("/dlq")
    public Map<String, Object> deadLetters() {
        List<Map<String, Object>> entries = dlqStore.recent();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("topic", dlqTopic);
        body.put("count", entries.size());
        body.put("deadLetters", entries);
        return body;
    }
}
