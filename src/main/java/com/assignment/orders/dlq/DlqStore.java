package com.assignment.orders.dlq;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Keeps the most recent dead letters in memory so they can be inspected over HTTP.
 *
 * This exists because Kafka message *headers* - which is where the failure reason lives -
 * are not exposed by the Confluent REST Proxy's consume API. Reading them here means the
 * whole DLQ story is visible from an HTTP client such as Insomnia.
 */
@Component
public class DlqStore {

    private static final int MAX_RETAINED = 100;

    private final Deque<Map<String, Object>> recent = new ConcurrentLinkedDeque<>();

    void add(Map<String, Object> entry) {
        recent.addFirst(entry);
        while (recent.size() > MAX_RETAINED) {
            recent.removeLast();
        }
    }

    public List<Map<String, Object>> recent() {
        return new ArrayList<>(recent);
    }

    public int size() {
        return recent.size();
    }
}
