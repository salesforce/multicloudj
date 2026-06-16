package com.salesforce.multicloudj.common.ali;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class AliRetryClassifierTest {

  @Test
  void status5xx_isRetryable() {
    assertEquals(Boolean.TRUE, AliRetryClassifier.classifyByStatusCode(500));
    assertEquals(Boolean.TRUE, AliRetryClassifier.classifyByStatusCode(503));
    assertEquals(Boolean.TRUE, AliRetryClassifier.classifyByStatusCode(599));
  }

  @Test
  void status408_isRetryable() {
    assertEquals(Boolean.TRUE, AliRetryClassifier.classifyByStatusCode(408));
  }

  @Test
  void otherClientErrors_areNonRetryable() {
    assertEquals(Boolean.FALSE, AliRetryClassifier.classifyByStatusCode(400));
    assertEquals(Boolean.FALSE, AliRetryClassifier.classifyByStatusCode(403));
    assertEquals(Boolean.FALSE, AliRetryClassifier.classifyByStatusCode(404));
  }

  @Test
  void unknownStatus_returnsNull() {
    assertNull(AliRetryClassifier.classifyByStatusCode(0));
    assertNull(AliRetryClassifier.classifyByStatusCode(200));
  }

  @Test
  void nullStatus_returnsNull() {
    assertNull(AliRetryClassifier.classifyByStatusCode((Integer) null));
  }

  @Test
  void boxedStatus_delegatesToPrimitive() {
    assertEquals(Boolean.TRUE, AliRetryClassifier.classifyByStatusCode(Integer.valueOf(503)));
    assertEquals(Boolean.FALSE, AliRetryClassifier.classifyByStatusCode(Integer.valueOf(404)));
  }

  @Test
  void throttlingErrorCodes_areRetryable() {
    assertEquals(
        Boolean.TRUE, AliRetryClassifier.classifyByThrottlingErrorCode("OTSServerBusy"));
    assertEquals(
        Boolean.TRUE,
        AliRetryClassifier.classifyByThrottlingErrorCode("OTSPartitionUnavailable"));
    assertEquals(
        Boolean.TRUE,
        AliRetryClassifier.classifyByThrottlingErrorCode("OTSQuotaExhausted"));
  }

  @Test
  void unknownErrorCode_returnsNull() {
    assertNull(AliRetryClassifier.classifyByThrottlingErrorCode("OTSInvalidParameter"));
    assertNull(AliRetryClassifier.classifyByThrottlingErrorCode(null));
  }
}
