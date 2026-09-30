package com.salesforce.multicloudj.common.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link ExceptionHandler#build}. */
class ExceptionHandlerTest {

  @Test
  void buildResourceAlreadyExistsException() {
    Throwable cause = new Throwable("Resource already exists");
    SubstrateSdkException result =
        ExceptionHandler.build(ResourceAlreadyExistsException.class, cause);
    assertInstanceOf(ResourceAlreadyExistsException.class, result);
    assertEquals(cause, result.getCause());
  }

  @Test
  void buildUnAuthorizedException() {
    Throwable cause = new Throwable("Unauthorized");
    SubstrateSdkException result = ExceptionHandler.build(UnAuthorizedException.class, cause);
    assertInstanceOf(UnAuthorizedException.class, result);
  }

  @Test
  void buildResourceExhaustedException() {
    Throwable cause = new Throwable("Resource exhausted");
    SubstrateSdkException result =
        ExceptionHandler.build(ResourceExhaustedException.class, cause);
    assertInstanceOf(ResourceExhaustedException.class, result);
  }

  @Test
  void buildInvalidArgumentException() {
    Throwable cause = new Throwable("Invalid argument");
    SubstrateSdkException result =
        ExceptionHandler.build(InvalidArgumentException.class, cause);
    assertInstanceOf(InvalidArgumentException.class, result);
  }

  @Test
  void buildPreservesAlreadyTypedException() {
    SubstrateSdkException original = new ResourceNotFoundException("not found");
    SubstrateSdkException result = ExceptionHandler.build(SubstrateSdkException.class, original);
    assertSame(original, result);
  }

  @Test
  void buildUnknownException() {
    Throwable cause = new Throwable("Unknown exception");
    SubstrateSdkException result = ExceptionHandler.build(UnknownException.class, cause);
    assertInstanceOf(UnknownException.class, result);
  }

  @Test
  void buildWithNullExceptionClassReturnsUnknown() {
    Throwable cause = new Throwable("Null exception type");
    SubstrateSdkException result = ExceptionHandler.build(null, cause);
    assertInstanceOf(UnknownException.class, result);
  }
}
