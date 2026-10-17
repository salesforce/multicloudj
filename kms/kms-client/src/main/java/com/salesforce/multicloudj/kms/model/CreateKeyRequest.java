package com.salesforce.multicloudj.kms.model;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/** Request to create a symmetric key. */
@Getter
@Builder
public class CreateKeyRequest {
  /**
   * Optional human-readable name of the key, unique within the client's key container. Providers
   * that assign their own key ids register it as an alias of the key; providers that require
   * caller-chosen key ids use it as the id, and for those it is required.
   */
  private final String keyName;

  @Builder.Default private final KeySpec keySpec = KeySpec.SYMMETRIC_DEFAULT;

  @Builder.Default private final KeyUsage keyUsage = KeyUsage.ENCRYPT_DECRYPT;

  /**
   * Source of the key material. With {@link KeyOrigin#EXTERNAL} the key is created in {@link
   * KeyState#PENDING_IMPORT} and becomes usable once material is imported.
   */
  @Builder.Default private final KeyOrigin origin = KeyOrigin.KMS;

  @Builder.Default private final ProtectionLevel protectionLevel = ProtectionLevel.SOFTWARE;

  private final String description;

  @Singular private final Map<String, String> tags;

  /**
   * Whether the key's material may be replicated to other regions. Providers that cannot replicate
   * key material reject {@code true} with {@code UnSupportedOperationException}.
   */
  private final boolean multiRegion;
}
