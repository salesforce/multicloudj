package com.salesforce.multicloudj.kms.model;

import java.time.Instant;
import lombok.Builder;
import lombok.Getter;

/** Request to import wrapped key material into a key created with {@link KeyOrigin#EXTERNAL}. */
@Getter
@Builder
public class ImportKeyMaterialRequest {
  private final KeyRef keyRef;

  /** Token from the {@link ImportParameters} whose public key wrapped the material. */
  private final String importToken;

  /** Key material wrapped with the import parameters' public key and algorithm. */
  private final byte[] wrappedKeyMaterial;

  @Builder.Default
  private final ExpirationModel expirationModel = ExpirationModel.KEY_MATERIAL_DOES_NOT_EXPIRE;

  /** Expiry of the material; required with {@link ExpirationModel#KEY_MATERIAL_EXPIRES}. */
  private final Instant validTo;
}
