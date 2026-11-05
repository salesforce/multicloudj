package com.salesforce.multicloudj.kms.model;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/** Request to encrypt a small payload directly with a key. */
@Getter
@Builder
public class EncryptRequest {
  private final KeyRef keyRef;
  private final byte[] plaintext;

  /**
   * Additional authenticated data bound to the ciphertext. The same entries must be supplied to
   * decrypt it.
   */
  @Singular("encryptionContextEntry")
  private final Map<String, String> encryptionContext;
}
