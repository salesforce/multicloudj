package com.salesforce.multicloudj.common.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ResourceConflictExceptionTest {

    @Test
    public void testDefaultConstructor() {
        ResourceConflictException exception = new ResourceConflictException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "Resource conflicts.";
        ResourceConflictException exception = new ResourceConflictException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        ResourceConflictException exception = new ResourceConflictException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "Resource conflicts.";
        Throwable cause = new Throwable("Cause");
        ResourceConflictException exception = new ResourceConflictException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}