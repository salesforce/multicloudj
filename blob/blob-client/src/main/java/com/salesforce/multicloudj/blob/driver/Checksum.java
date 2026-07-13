package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Checksum reported by the cloud store for a blob.
 *
 * <p>Carries the algorithm and the checksum value produced by that algorithm. The value format is
 * provider-specific and should be treated as an opaque string - use it with the same provider that
 * produced it.
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class Checksum {
  private final ChecksumMethod algorithm;
  private final String value;
}
