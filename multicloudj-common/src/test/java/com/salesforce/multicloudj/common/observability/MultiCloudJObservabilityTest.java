package com.salesforce.multicloudj.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class MultiCloudJObservabilityTest {

  @AfterEach
  void resetGlobalDefault() {
    MultiCloudJObservability.setDefaultTracingPolicy(TracingPolicy.DISABLED);
  }

  @Test
  void initialDefaultIsDisabled() {
    assertSame(TracingPolicy.DISABLED, MultiCloudJObservability.getDefaultTracingPolicy());
  }

  @Test
  void setDefaultPolicy_storedAndReturned() {
    MultiCloudJObservability.setDefaultTracingPolicy(TracingPolicy.JOIN_ONLY);
    assertSame(TracingPolicy.JOIN_ONLY, MultiCloudJObservability.getDefaultTracingPolicy());

    MultiCloudJObservability.setDefaultTracingPolicy(TracingPolicy.CHILD_AND_ROOT);
    assertSame(TracingPolicy.CHILD_AND_ROOT, MultiCloudJObservability.getDefaultTracingPolicy());
  }

  @Test
  void setDefaultPolicy_nullCoercedToDisabled() {
    MultiCloudJObservability.setDefaultTracingPolicy(TracingPolicy.JOIN_ONLY);
    MultiCloudJObservability.setDefaultTracingPolicy(null);
    assertEquals(TracingPolicy.DISABLED, MultiCloudJObservability.getDefaultTracingPolicy());
  }
}
