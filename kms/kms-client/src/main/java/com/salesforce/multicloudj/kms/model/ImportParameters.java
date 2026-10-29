package com.salesforce.multicloudj.kms.model;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

/**
 * Parameters for importing key material: the caller wraps the material with {@link
 * #getWrappingPublicKey()} using {@link #getWrappingAlgorithm()} and passes the result, together
 * with {@link #getImportToken()}, to {@code importKeyMaterial} before {@link #getTokenExpiry()}.
 */
@Getter
@Builder
public class ImportParameters {
  private final KeyRef keyRef;

  /** DER-encoded X.509 SubjectPublicKeyInfo of the RSA wrapping key. */
  private final byte[] wrappingPublicKey;

  /** Opaque token identifying this import session; pass it back unchanged. */
  private final String importToken;

  private final WrappingAlgorithm wrappingAlgorithm;
  private final WrappingKeySpec wrappingKeySpec;
  private final Instant tokenExpiry;
}
