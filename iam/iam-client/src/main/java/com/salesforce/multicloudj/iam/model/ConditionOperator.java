package com.salesforce.multicloudj.iam.model;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Condition operators for IAM policy conditions.
 *
 * <p>These operators are used to define conditions under which a policy statement applies. They
 * follow substrate-neutral naming conventions and are translated to provider-specific formats
 * (e.g., "stringEquals" → "StringEquals" in AWS).
 */
public enum ConditionOperator {
  /** String equality comparison */
  STRING_EQUALS("stringEquals"),

  /** String inequality comparison */
  STRING_NOT_EQUALS("stringNotEquals"),

  /** String pattern matching (supports wildcards) */
  STRING_LIKE("stringLike"),

  /** Negated string pattern matching */
  STRING_NOT_LIKE("stringNotLike"),

  /** Numeric equality comparison */
  NUMERIC_EQUALS("numericEquals"),

  /** Numeric inequality comparison */
  NUMERIC_NOT_EQUALS("numericNotEquals"),

  /** Numeric less-than comparison */
  NUMERIC_LESS_THAN("numericLessThan"),

  /** Numeric less-than-or-equal comparison */
  NUMERIC_LESS_THAN_EQUALS("numericLessThanEquals"),

  /** Numeric greater-than comparison */
  NUMERIC_GREATER_THAN("numericGreaterThan"),

  /** Numeric greater-than-or-equal comparison */
  NUMERIC_GREATER_THAN_EQUALS("numericGreaterThanEquals"),

  /** Date equality comparison */
  DATE_EQUALS("dateEquals"),

  /** Date inequality comparison */
  DATE_NOT_EQUALS("dateNotEquals"),

  /** Date less-than (before) comparison */
  DATE_LESS_THAN("dateLessThan"),

  /** Date less-than-or-equal comparison */
  DATE_LESS_THAN_EQUALS("dateLessThanEquals"),

  /** Date greater-than (after) comparison */
  DATE_GREATER_THAN("dateGreaterThan"),

  /** Date greater-than-or-equal comparison */
  DATE_GREATER_THAN_EQUALS("dateGreaterThanEquals"),

  /** Boolean comparison */
  BOOL("bool"),

  /** IP address matching */
  IP_ADDRESS("ipAddress"),

  /** Negated IP address matching */
  NOT_IP_ADDRESS("notIpAddress");

  private final String value;

  ConditionOperator(String value) {
    this.value = value;
  }

  /**
   * Returns the string representation used in policy documents.
   *
   * @return the operator value
   */
  @JsonValue
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return value;
  }
}
