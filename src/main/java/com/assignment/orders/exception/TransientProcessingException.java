package com.assignment.orders.exception;

/**
 * A temporary failure (network blip, downstream service timeout, lock contention...).
 * The consumer RETRIES these with exponential backoff; only if every attempt fails
 * does the message end up in the Dead Letter Queue.
 */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
