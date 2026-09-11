package com.assignment.orders.aggregation;

import com.assignment.orders.avro.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REAL-TIME AGGREGATION.
 *
 * Maintains a running average of order prices, updated on every successfully processed
 * message - overall and broken down per product. The average is computed incrementally
 * from a running count and sum, so it is O(1) per message and never needs to re-read
 * history.
 *
 * Only successfully processed orders are counted: retried-then-recovered orders are
 * counted exactly once, and orders that end up in the DLQ are never counted.
 */
@Component
public class PriceAggregator {

    private static final Logger log = LoggerFactory.getLogger(PriceAggregator.class);

    private final AtomicLong count = new AtomicLong();
    private final Object lock = new Object();
    private double sum;
    private double min = Double.MAX_VALUE;
    private double max = Double.MIN_VALUE;

    private final Map<String, ProductStats> perProduct = new ConcurrentHashMap<>();

    /** Folds one order into the running statistics and returns the new global average. */
    public double record(Order order) {
        double price = order.getPrice();
        long n;
        double average;

        synchronized (lock) {
            n = count.incrementAndGet();
            sum += price;
            min = Math.min(min, price);
            max = Math.max(max, price);
            average = sum / n;
        }

        perProduct.computeIfAbsent(order.getProduct(), k -> new ProductStats()).add(price);
        return average;
    }

    public long getCount() {
        return count.get();
    }

    public double getAverage() {
        synchronized (lock) {
            return count.get() == 0 ? 0d : sum / count.get();
        }
    }

    /** Full snapshot of the running aggregation, as served by GET /api/stats. */
    public Map<String, Object> snapshot() {
        Map<String, Object> out = new LinkedHashMap<>();
        long n = count.get();
        synchronized (lock) {
            out.put("ordersProcessed", n);
            out.put("totalValue", round(sum));
            out.put("runningAverage", round(n == 0 ? 0d : sum / n));
            out.put("minPrice", round(n == 0 ? 0d : min));
            out.put("maxPrice", round(n == 0 ? 0d : max));
        }
        Map<String, Object> byProduct = new LinkedHashMap<>();
        perProduct.entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                .forEach(e -> {
                    Map<String, Object> stats = new LinkedHashMap<>();
                    stats.put("count", e.getValue().count);
                    stats.put("average", round(e.getValue().average()));
                    byProduct.put(e.getKey(), stats);
                });
        out.put("byProduct", byProduct);
        return out;
    }

    private static double round(double d) {
        return Math.round(d * 100d) / 100d;
    }

    /** Periodic snapshot - the "dashboard" for the live demo. */
    @Scheduled(fixedDelay = 15_000, initialDelay = 15_000)
    public void printSummary() {
        long n = count.get();
        if (n == 0) {
            return;
        }
        synchronized (lock) {
            log.info("");
            log.info("================ RUNNING AGGREGATION ================");
            log.info(" orders processed : {}", n);
            log.info(" total value      : {}", fmt(sum));
            log.info(" RUNNING AVERAGE  : {}", fmt(sum / n));
            log.info(" min / max price  : {} / {}", fmt(min), fmt(max));
            log.info(" ---- per product ----");
            perProduct.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(Comparator.naturalOrder()))
                    .forEach(e -> log.info("  {} count={} avg={}",
                            pad(e.getKey()), e.getValue().count, fmt(e.getValue().average())));
            log.info("=====================================================");
            log.info("");
        }
    }

    private static String pad(String s) {
        return String.format("%-12s", s);
    }

    private static String fmt(double d) {
        return String.format("%.2f", d);
    }

    private static final class ProductStats {
        private long count;
        private double sum;

        synchronized void add(double price) {
            count++;
            sum += price;
        }

        synchronized double average() {
            return count == 0 ? 0d : sum / count;
        }
    }
}
