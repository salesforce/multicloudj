package com.salesforce.multicloudj.kms.model;

/** Algorithm used to wrap (encrypt) key material before it is imported. */
public enum WrappingAlgorithm {
  /** RSA-OAEP with SHA-256 applied directly to the key material. */
  RSAES_OAEP_SHA_256,

  /**
   * RSA-OAEP with SHA-256 wrapping an ephemeral AES-256 key, which in turn wraps the key material
   * with AES key wrap with padding (RFC 5649).
   */
  RSA_AES_KEY_WRAP_SHA_256
}
