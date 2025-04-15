package com.salesforce.multicloudj.common.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class UnSupportedExceptionTest {

    @Test
    public void testDefaultConstructor() {
        UnSupportedOperationException exception = new UnSupportedOperationException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "Unsupported operation";
        UnSupportedOperationException exception = new UnSupportedOperationException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        UnSupportedOperationException exception = new UnSupportedOperationException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "UnSupported operation";
        Throwable cause = new Throwable("Cause");
        UnSupportedOperationException exception = new UnSupportedOperationException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}