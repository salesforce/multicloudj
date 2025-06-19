package com.salesforce.multicloudj.common.exceptions;

/**
 * Exception thrown when a transaction fails due to conflicts, cancellations, or other transaction-related issues.
 * <p>
 * This exception is used across different cloud providers to represent transaction failures:
 */
public class TransactionFailedException extends SubstrateSdkException {

    public TransactionFailedException() {
        super();
    }

    public TransactionFailedException(String message, Throwable cause) {
        super(message, cause);
    }

    public TransactionFailedException(String message) {
        super(message);
    }

    public TransactionFailedException(Throwable cause) {
        super(cause);
    }
} 