package com.salesforce.multicloudj.kms.model;

import lombok.Builder;
import lombok.Getter;

/** Result of encrypting a payload. */
@Getter
@Builder
public class EncryptResponse {
  private final byte[] ciphertext;

  /** Key, including the version where the provider versions keys, that produced the ciphertext. */
  private final KeyRef keyRef;
}
