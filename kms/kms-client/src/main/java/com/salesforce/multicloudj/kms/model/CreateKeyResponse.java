package com.salesforce.multicloudj.kms.model;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

/** Result of creating a key. */
@Getter
@Builder
public class CreateKeyResponse {
  private final KeyRef keyRef;
  private final KeyState keyState;
  private final Instant creationTime;
  private final String description;
}
