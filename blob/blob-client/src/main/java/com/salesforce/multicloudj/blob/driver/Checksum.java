package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Checksum reported by the cloud store for a blob.
 *
 * <p>Carries the algorithm and the Base64-encoded checksum value produced by that algorithm. The
 * algorithm is drawn from {@link ChecksumMethod}; the value's format is defined by that algorithm.
 */
@Builder
@Getter
@EqualsAndHashCode
@ToString
public class Checksum {
  private final ChecksumMethod algorithm;
  private final String value;
}
