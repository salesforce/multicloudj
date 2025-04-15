package com.salesforce.multicloudj.common.exceptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ResourceAlreadyExistsExceptionTest {

    @Test
    public void testDefaultConstructor() {
        ResourceAlreadyExistsException exception = new ResourceAlreadyExistsException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "Resource already exists";
        ResourceAlreadyExistsException exception = new ResourceAlreadyExistsException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        ResourceAlreadyExistsException exception = new ResourceAlreadyExistsException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "Resource already exists";
        Throwable cause = new Throwable("Cause");
        ResourceAlreadyExistsException exception = new ResourceAlreadyExistsException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}