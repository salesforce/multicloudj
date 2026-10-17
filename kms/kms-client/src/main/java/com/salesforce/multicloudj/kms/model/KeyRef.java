package com.salesforce.multicloudj.kms.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Reference to a key, or to a single version of a key, in the key management service.
 *
 * <p>A {@code KeyRef} returned by the SDK round-trips: passing it back to any operation addresses
 * the same key. When constructing one by hand only {@link #getKeyId()} is required.
 */
@Getter
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class KeyRef {
  /** Provider that owns the key, such as the id passed to {@code KmsClient.builder}. */
  private final String providerId;

  private final String region;

  /**
   * Native identifier of the key. Either a short id, which the provider resolves against the
   * client's region and key container, or a fully qualified native resource name.
   */
  private final String keyId;

  /** Fully qualified native resource name of the key, when the provider reports one. */
  private final String resourceName;

  /**
   * Version of the key material, for providers that version keys. When absent, operations address
   * the key as a whole.
   */
  private final String versionId;
}
