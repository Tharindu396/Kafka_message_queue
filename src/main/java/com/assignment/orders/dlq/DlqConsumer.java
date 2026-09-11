package com.assignment.orders.dlq;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DEAD LETTER QUEUE INSPECTOR.
 *
 * Tails orders.DLQ and prints, for every parked message, what it was and why it failed.
 * Run it on its own with:  -Dspring-boot.run.profiles=dlq
 */
@Component
@Profile("dlq")
public class DlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(DlqConsumer.class);

    /** Headers that carry the failure context (Spring's kafka_dlt-* plus our own x-*). */
    private static final String[] INTERESTING_HEADERS = {
            "x-failure-class",
            "kafka_dlt-original-topic",
            "kafka_dlt-original-partition",
            "kafka_dlt-original-offset",
            "kafka_dlt-exception-fqcn",
            "kafka_dlt-exception-message",
            "x-failed-at"
    };

    private final KafkaAvroDeserializer decoder;
    private final DlqStore store;
    private final AtomicLong dlqCount = new AtomicLong();

    public DlqConsumer(KafkaAvroDeserializer dlqAvroDecoder, DlqStore store) {
        this.decoder = dlqAvroDecoder;
        this.store = store;
    }

    @KafkaListener(
            topics = "${app.topics.dlq}",
            groupId = "orders-dlq-monitor",
            containerFactory = "dlqListenerContainerFactory")
    public void onDeadLetter(ConsumerRecord<String, byte[]> record) {
        long total = dlqCount.incrementAndGet();
        String payload = decode(record);

        Map<String, Object> failure = new LinkedHashMap<>();
        for (String name : INTERESTING_HEADERS) {
            Header header = record.headers().lastHeader(name);
            if (header != null) {
                failure.put(name, asText(header));
            }
        }

        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("sequence", total);
        entry.put("key", record.key());
        entry.put("payload", payload);
        entry.put("dlqPartition", record.partition());
        entry.put("dlqOffset", record.offset());
        entry.put("failure", failure);
        store.add(entry);

        log.error("");
        log.error("**************** DEAD LETTER #{} ****************", total);
        log.error("  key      : {}", record.key());
        log.error("  payload  : {}", payload);
        failure.forEach((name, value) -> log.error("  {} : {}", String.format("%-28s", name), value));
        log.error("************************************************");
        log.error("");
    }

    private String decode(ConsumerRecord<String, byte[]> record) {
        if (record.value() == null) {
            return "<null>";
        }
        try {
            return String.valueOf(decoder.deserialize(record.topic(), record.value()));
        } catch (Exception ex) {
            return "<undecodable: " + ex.getClass().getSimpleName() + " - "
                    + record.value().length + " raw bytes>";
        }
    }

    private static String asText(Header header) {
        // kafka_dlt-original-partition/offset are binary (int/long), the rest are UTF-8.
        byte[] value = header.value();
        if (value.length == 4) {
            return String.valueOf(java.nio.ByteBuffer.wrap(value).getInt());
        }
        if (value.length == 8) {
            return String.valueOf(java.nio.ByteBuffer.wrap(value).getLong());
        }
        return new String(value, StandardCharsets.UTF_8);
    }
}
