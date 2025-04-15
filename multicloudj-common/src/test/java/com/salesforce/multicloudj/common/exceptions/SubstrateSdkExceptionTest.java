package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class SubstrateSdkExceptionTest {

    @Test
    public void testDefaultConstructor() {
        SubstrateSdkException exception = new SubstrateSdkException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "some exception";
        SubstrateSdkException exception = new SubstrateSdkException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        SubstrateSdkException exception = new SubstrateSdkException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "some exception";
        Throwable cause = new Throwable("Cause");
        SubstrateSdkException exception = new SubstrateSdkException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}