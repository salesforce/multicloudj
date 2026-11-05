package com.salesforce.multicloudj.kms.model;

import lombok.Builder;
import lombok.Getter;

/** Result of decrypting a ciphertext. */
@Getter
@Builder
public class DecryptResponse {
  private final byte[] plaintext;

  /** Key that decrypted the ciphertext, when the provider reports it. */
  private final KeyRef keyRef;
}
