package com.salesforce.multicloudj.common.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.BaseServiceException;
import org.junit.jupiter.api.Test;

class GcpRetryClassifierTest {

  @Test
  void retryableApiException_returnsTrue() {
    ApiException e = new ApiException(null, FakeStatusCode.UNAVAILABLE, true);
    assertEquals(Boolean.TRUE, GcpRetryClassifier.classify(e));
  }

  @Test
  void nonRetryableApiException_returnsFalse() {
    ApiException e = new ApiException(null, FakeStatusCode.INVALID_ARGUMENT, false);
    assertEquals(Boolean.FALSE, GcpRetryClassifier.classify(e));
  }

  @Test
  void retryableBaseServiceException_returnsTrue() {
    BaseServiceException e = new TestBaseServiceException(503, "service unavailable", true);
    assertEquals(Boolean.TRUE, GcpRetryClassifier.classify(e));
  }

  @Test
  void nonRetryableBaseServiceException_returnsFalse() {
    BaseServiceException e = new TestBaseServiceException(404, "not found", false);
    assertEquals(Boolean.FALSE, GcpRetryClassifier.classify(e));
  }

  @Test
  void wrappedBaseServiceException_isDetected() {
    BaseServiceException retryable = new TestBaseServiceException(503, "retry me", true);
    RuntimeException wrapper = new RuntimeException("outer", retryable);
    assertEquals(Boolean.TRUE, GcpRetryClassifier.classify(wrapper));
  }

  @Test
  void unrelatedException_returnsNull() {
    assertNull(GcpRetryClassifier.classify(new IllegalStateException("nope")));
  }

  @Test
  void nullThrowable_returnsNull() {
    assertNull(GcpRetryClassifier.classify(null));
  }

  @Test
  void wrappedApiException_isDetected() {
    ApiException retryable = new ApiException(null, FakeStatusCode.UNAVAILABLE, true);
    RuntimeException wrapper = new RuntimeException("outer", retryable);
    assertEquals(Boolean.TRUE, GcpRetryClassifier.classify(wrapper));
  }

  /** Minimal StatusCode implementation since gax provides only an enum/interface. */
  private static class FakeStatusCode implements StatusCode {
    static final FakeStatusCode UNAVAILABLE = new FakeStatusCode(Code.UNAVAILABLE);
    static final FakeStatusCode INVALID_ARGUMENT = new FakeStatusCode(Code.INVALID_ARGUMENT);

    private final Code code;

    FakeStatusCode(Code code) {
      this.code = code;
    }

    @Override
    public Code getCode() {
      return code;
    }

    @Override
    public Object getTransportCode() {
      return code;
    }
  }

  /**
   * Concrete test double for {@link BaseServiceException}, which has a protected constructor and is
   * normally subclassed by every {@code google-cloud-*} service. This stand-in exercises the
   * classifier's polymorphic dispatch on any {@code BaseServiceException} subtype without pulling
   * in a service-specific dep.
   */
  private static class TestBaseServiceException extends BaseServiceException {
    TestBaseServiceException(int code, String message, boolean retryable) {
      super(BaseServiceException.ExceptionData.from(code, message, null, retryable));
    }
  }
}
