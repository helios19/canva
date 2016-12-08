package com.canva.queue.common.exception;

/**
 * Global Queue Service unchecked exception class.
 */
public class QueueServiceException extends RuntimeException {
    public QueueServiceException() {
        super();
    }

    public QueueServiceException(String message) {
        super(message);
    }

    public QueueServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
