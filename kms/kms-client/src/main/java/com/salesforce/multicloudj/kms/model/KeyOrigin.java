package com.salesforce.multicloudj.kms.model;

/** Source of a key's material. */
public enum KeyOrigin {
  /** The key service generates the key material. */
  KMS,

  /**
   * The key is created without material, which is supplied later through {@code
   * getImportParameters} and {@code importKeyMaterial} (bring your own key).
   */
  EXTERNAL
}
