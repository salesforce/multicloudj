package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class TransactionFailedExceptionTest {

    @Test
    public void testDefaultConstructor() {
        TransactionFailedException exception = new TransactionFailedException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "Transaction failed due to conflict";
        TransactionFailedException exception = new TransactionFailedException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Transaction cancelled");
        TransactionFailedException exception = new TransactionFailedException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Transaction cancelled", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "Atomic write transaction failed";
        Throwable cause = new Throwable("Transaction cancelled");
        TransactionFailedException exception = new TransactionFailedException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    public void testExceptionHierarchy() {
        TransactionFailedException exception = new TransactionFailedException("test");
        assertEquals(SubstrateSdkException.class, exception.getClass().getSuperclass());
    }
} 