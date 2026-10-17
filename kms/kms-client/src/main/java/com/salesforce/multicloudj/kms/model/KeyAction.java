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
  /** Administrative access: changing the key's policy, lifecycle state, and tags. */
  MANAGE
}
