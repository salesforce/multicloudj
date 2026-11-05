package com.salesforce.multicloudj.kms.model;

/** Request attribute evaluated by a {@link KeyCondition}. */
public enum ConditionAttribute {
  /** Fully qualified native name of the calling identity. */
  PRINCIPAL_NAME,

  /**
   * One entry of the request's encryption context, named by {@link KeyCondition#getKey()}. Only
   * meaningful for cryptographic actions; grants combining it with other actions are rejected.
   */
  ENCRYPTION_CONTEXT,

  /** Fully qualified native name of the resource a cloud service is acting on behalf of. */
  SOURCE_RESOURCE,

  /** Account or project that owns the resource a cloud service is acting on behalf of. */
  SOURCE_ACCOUNT,

  /**
   * A provider-native attribute named verbatim by {@link KeyCondition#getKey()}. Escape hatch for
   * attributes with no portable equivalent; the policy is then provider-specific.
   */
  NATIVE
}
