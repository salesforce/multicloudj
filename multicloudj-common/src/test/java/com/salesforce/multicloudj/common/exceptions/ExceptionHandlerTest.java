package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/**
 * Test class for ExceptionHandler.
 */
public class ExceptionHandlerTest {

    @Test
    public void testHandleAndPropagateResourceAlreadyExistsException() {
        Throwable t = new Throwable("Resource already exists");
        assertThrows(ResourceAlreadyExistsException.class,
                () -> ExceptionHandler.handleAndPropagate(ResourceAlreadyExistsException.class, t));
    }

    @Test
    public void testHandleAndPropagateUnAuthorizedException() {
        Throwable t = new Throwable("Unauthorized");
        assertThrows(UnAuthorizedException.class,
                () -> ExceptionHandler.handleAndPropagate(UnAuthorizedException.class, t));
    }

    @Test
    public void testHandleAndPropagateResourceExhaustedException() {
        Throwable t = new Throwable("Resource exhausted");
        assertThrows(ResourceExhaustedException.class,
                () -> ExceptionHandler.handleAndPropagate(ResourceExhaustedException.class, t));
    }

    @Test
    public void testHandleAndPropagateInvalidArgumentException() {
        Throwable t = new Throwable("Invalid argument");
        assertThrows(InvalidArgumentException.class,
                () -> ExceptionHandler.handleAndPropagate(InvalidArgumentException.class, t));
    }

    @Test
    public void testHandleAndPropagateSubstrateSdkException() {
        SubstrateSdkException t = new SubstrateSdkException("Substrate SDK exception");
        assertThrows(SubstrateSdkException.class,
                () -> ExceptionHandler.handleAndPropagate(SubstrateSdkException.class, t));
    }

    @Test
    public void testHandleAndPropagateUnknownException() {
        Throwable t = new Throwable("Unknown exception");
        assertThrows(UnknownException.class,
                () -> ExceptionHandler.handleAndPropagate(UnknownException.class, t));
    }

    @Test
    public void testNullExceptionTypeUnknownException() {
        Throwable t = new Throwable("Null exception type");
        assertThrows(UnknownException.class, () -> ExceptionHandler.handleAndPropagate(null, t));
    }
}