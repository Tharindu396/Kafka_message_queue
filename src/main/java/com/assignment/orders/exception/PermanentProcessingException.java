package com.assignment.orders.exception;

/**
 * A failure that will never succeed no matter how many times we try
 * (invalid data, business-rule violation). Retrying would just waste time,
 * so these go STRAIGHT to the Dead Letter Queue.
 */
public class PermanentProcessingException extends RuntimeException {
    public PermanentProcessingException(String message) {
        super(message);
    }
}
