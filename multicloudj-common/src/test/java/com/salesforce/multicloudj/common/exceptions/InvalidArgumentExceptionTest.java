package com.salesforce.multicloudj.common.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class InvalidArgumentExceptionTest {

    @Test
    public void testDefaultConstructor() {
        InvalidArgumentException exception = new InvalidArgumentException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "Invalid argument";
        InvalidArgumentException exception = new InvalidArgumentException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        InvalidArgumentException exception = new InvalidArgumentException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "Invalid argument";
        Throwable cause = new Throwable("Cause");
        InvalidArgumentException exception = new InvalidArgumentException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}