package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

public class UnAuthorizedExceptionTest {

    @Test
    public void testDefaultConstructor() {
        UnAuthorizedException exception = new UnAuthorizedException();
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "unauthorized exception";
        UnAuthorizedException exception = new UnAuthorizedException(message);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    public void testConstructorWithCause() {
        Throwable cause = new Throwable("Cause");
        UnAuthorizedException exception = new UnAuthorizedException(cause);
        assertEquals(cause, exception.getCause());
        assertEquals("java.lang.Throwable: Cause", exception.getMessage());
    }

    @Test
    public void testConstructorWithMessageAndCause() {
        String message = "unauthorized exception";
        Throwable cause = new Throwable("Cause");
        UnAuthorizedException exception = new UnAuthorizedException(message, cause);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}