package com.salesforce.multicloudj.common.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.core.exception.SdkClientException;

class AwsRetryClassifierTest {

  @Test
  void serviceException5xx_isRetryable() {
    AwsServiceException e =
        AwsServiceException.builder()
            .statusCode(503)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ServiceUnavailable").build())
            .build();
    assertEquals(Boolean.TRUE, AwsRetryClassifier.classify(e));
  }

  @Test
  void serviceException4xx_isNonRetryable() {
    AwsServiceException e =
        AwsServiceException.builder()
            .statusCode(400)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ValidationError").build())
            .build();
    assertEquals(Boolean.FALSE, AwsRetryClassifier.classify(e));
  }

  @Test
  void serviceException408_isRetryable() {
    AwsServiceException e =
        AwsServiceException.builder()
            .statusCode(408)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("RequestTimeout").build())
            .build();
    assertEquals(Boolean.TRUE, AwsRetryClassifier.classify(e));
  }

  @Test
  void retryableExceptionMarker_isRetryable() {
    RetryableException e = RetryableException.builder().message("transient").build();
    assertEquals(Boolean.TRUE, AwsRetryClassifier.classify(e));
  }

  @Test
  void sdkClientException_isRetryable() {
    SdkClientException e = SdkClientException.builder().message("connection reset").build();
    assertEquals(Boolean.TRUE, AwsRetryClassifier.classify(e));
  }

  @Test
  void unrelatedException_returnsNull() {
    assertNull(AwsRetryClassifier.classify(new IllegalStateException("nope")));
  }

  @Test
  void nullThrowable_returnsNull() {
    assertNull(AwsRetryClassifier.classify(null));
  }

  @Test
  void wrappedRetryableInCauseChain_isDetected() {
    AwsServiceException retryable =
        AwsServiceException.builder()
            .statusCode(503)
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ServiceUnavailable").build())
            .build();
    RuntimeException wrapper = new RuntimeException("outer", retryable);
    assertTrue(AwsRetryClassifier.classify(wrapper));
  }
}
