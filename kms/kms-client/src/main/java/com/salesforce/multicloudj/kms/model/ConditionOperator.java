package com.salesforce.multicloudj.kms.model;

/** Comparison applied by a {@link KeyCondition} to a request attribute. */
public enum ConditionOperator {
  STRING_EQUALS,
  STRING_NOT_EQUALS,
  /** String match where the condition values may contain {@code *} and {@code ?} wildcards. */
  STRING_LIKE,
  STRING_NOT_LIKE,
  /** Exact match against a fully qualified native resource name. */
  RESOURCE_NAME_EQUALS,
  /**
   * Match against a fully qualified native resource name where the condition values may contain
   * {@code *} and {@code ?} wildcards.
   */
  RESOURCE_NAME_LIKE,
  BOOL
}
