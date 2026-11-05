package com.salesforce.multicloudj.kms.model;

/** Lifecycle state of a key. */
public enum KeyState {
  CREATING,
  ENABLED,
  DISABLED,
  /** The key has no usable material yet and is waiting for imported material. */
  PENDING_IMPORT,
  PENDING_DELETION,
  UNAVAILABLE,
  /** The provider reported a state that has no equivalent in this enum. */
  UNKNOWN
}
