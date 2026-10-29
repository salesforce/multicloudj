package com.salesforce.multicloudj.kms.model;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/** Request to decrypt a ciphertext produced by {@code encrypt}. */
@Getter
@Builder
public class DecryptRequest {
  /**
   * Key that produced the ciphertext. Some providers locate the key from the ciphertext itself;
   * providers that cannot reject a missing key with {@code InvalidArgumentException}.
   */
  private final KeyRef keyRef;

  private final byte[] ciphertext;

  @Singular("encryptionContextEntry")
  private final Map<String, String> encryptionContext;
}
