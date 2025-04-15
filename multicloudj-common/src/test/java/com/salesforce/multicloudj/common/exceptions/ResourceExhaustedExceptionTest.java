package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class ResourceExhaustedExceptionTest {

    @Test
    public void testDefaultConstructor() {
        ResourceExhaustedException exception = new ResourceExhaustedException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "resource exhausted";
        ResourceExhaustedException exception = new ResourceExhaustedException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        ResourceExhaustedException exception = new ResourceExhaustedException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "resource exhausted";
        Throwable cause = new Throwable("Cause");
        ResourceExhaustedException exception = new ResourceExhaustedException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}