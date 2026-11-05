package com.salesforce.multicloudj.kms.model;

/** Portable permissions that a {@link KeyGrant} can give on a key. */
public enum KeyAction {
  ENCRYPT,
  DECRYPT,
  GENERATE_DATA_KEY,
  CREATE_GRANT,
  REENCRYPT,
  SIGN,
  VERIFY,
  DESCRIBE,

  /**
   * Full administrative access as the provider defines a key administrator: changing the key's
   * policy, lifecycle state and tags, and scheduling its deletion. Grants no cryptographic use.
   */
  MANAGE,

  /** Enabling and disabling the key. */
  MANAGE_STATE,

  /** Reading and replacing the key's access policy. */
  MANAGE_POLICY,

  /** Adding and removing the key's tags. */
  MANAGE_TAGS,

  /** Starting import sessions and importing key material into the key. */
  IMPORT_KEY_MATERIAL,

  /** Replicating the key to other regions and changing which replica is primary. */
  REPLICATE,

  /**
   * Every operation on the key, including ones this enum does not name. Providers without a single
   * native "everything" permission grant the closest combination their permission model offers.
   */
  ALL
}
