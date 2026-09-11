package com.assignment.orders.config;

import com.assignment.orders.exception.PermanentProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RETRY + DEAD LETTER QUEUE.
 *
 * <pre>
 *   listener throws
 *        |
 *        +-- PermanentProcessingException / DeserializationException
 *        |        -> not retryable -> published to orders.DLQ immediately
 *        |
 *        +-- TransientProcessingException (or anything else)
 *                 -> retried in place with exponential backoff 0.5s, 1s, 2s
 *                 -> still failing after 3 retries -> published to orders.DLQ
 * </pre>
 *
 * Because the container uses manual (RECORD ack-mode) commits, the offset is only
 * committed once the record has either succeeded or been safely parked in the DLQ.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlingConfig.class);

    @Value("${app.topics.dlq}")
    private String dlqTopic;

    @Value("${app.retry.max-retries}")
    private int maxRetries;

    @Value("${app.retry.initial-interval-ms}")
    private long initialInterval;

    @Value("${app.retry.multiplier}")
    private double multiplier;

    @Value("${app.retry.max-interval-ms}")
    private long maxInterval;

    /**
     * Republishes a failed record onto the DLQ, keeping the original key and partition
     * and attaching diagnostic headers (original topic/partition/offset, exception type
     * and message, plus our own failure classification).
     */
    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            KafkaTemplate<String, Object> avroKafkaTemplate,
            KafkaTemplate<String, byte[]> bytesKafkaTemplate) {

        // A record that failed *deserialisation* only exists as raw bytes, so it needs a
        // byte[] template; everything else is a properly deserialised Avro Order.
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesKafkaTemplate);
        templates.put(Object.class, avroKafkaTemplate);

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                templates,
                (record, ex) -> new TopicPartition(dlqTopic, record.partition()));

        recoverer.setHeadersFunction((record, ex) -> {
            List<Header> extra = new ArrayList<>();
            extra.add(header("x-failure-class", classify(ex)));
            extra.add(header("x-failed-at", java.time.Instant.now().toString()));
            extra.add(header("x-original-key", String.valueOf(record.key())));
            return new org.apache.kafka.common.header.internals.RecordHeaders(extra);
        });

        return recoverer;
    }

    @Bean
    CommonErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialInterval);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxInterval);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // These will never succeed - do not waste time retrying, go straight to the DLQ.
        handler.addNotRetryableExceptions(
                PermanentProcessingException.class,
                DeserializationException.class);

        // Spring logs a "Seek to current after exception" stack trace on every retry;
        // demote it so the console shows our own readable RETRY/DLQ lines instead.
        handler.setLogLevel(KafkaException.Level.DEBUG);

        handler.setRetryListeners(new RetryListener() {
            @Override
            public void failedDelivery(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt) {
                if (isRetryable(ex)) {
                    log.warn("RETRY   | attempt {}/{} failed for key={} offset={} : {}",
                            deliveryAttempt, maxRetries + 1, record.key(), record.offset(), rootMessage(ex));
                } else {
                    log.warn("NO-RETRY| key={} offset={} is unrecoverable, skipping straight to DLQ : {}",
                            record.key(), record.offset(), rootMessage(ex));
                }
            }

            @Override
            public void recovered(ConsumerRecord<?, ?> record, Exception ex) {
                log.error("DLQ     | giving up on key={} offset={} -> {} : {}",
                        record.key(), record.offset(), dlqTopic, rootMessage(ex));
            }
        });

        return handler;
    }

    private static Header header(String key, String value) {
        return new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8));
    }

    /** Transient failures are retried; permanent ones and poison pills are not. */
    private static boolean isRetryable(Exception ex) {
        return !hasCause(ex, PermanentProcessingException.class)
                && !hasCause(ex, DeserializationException.class);
    }

    private static String classify(Exception ex) {
        if (hasCause(ex, PermanentProcessingException.class)) {
            return "PERMANENT";
        }
        if (hasCause(ex, DeserializationException.class)) {
            return "POISON_PILL";
        }
        return "TRANSIENT_RETRIES_EXHAUSTED";
    }

    /**
     * Searches the whole cause chain, not just the root: a DeserializationException wraps
     * the Avro/Confluent failure that actually caused it, so the root cause is an
     * ArrayIndexOutOfBounds or SerializationException rather than the class we classify on.
     */
    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        Throwable current = t;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return false;
    }

    private static String rootMessage(Exception ex) {
        // For a poison pill the root cause is the low-level parse error - report the
        // deserialization failure itself, which is the actionable part.
        if (hasCause(ex, DeserializationException.class)) {
            return "DeserializationException: failed to deserialize the record value";
        }
        Throwable cause = rootCause(ex);
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    private static Throwable rootCause(Throwable t) {
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
