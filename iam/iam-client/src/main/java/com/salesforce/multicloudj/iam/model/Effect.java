package com.salesforce.multicloudj.iam.model;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Represents the effect of a policy statement.
 *
 * <p>Effect determines whether the statement allows or denies access.
 */
public enum Effect {
  /** Grants access to the specified actions */
  ALLOW("Allow"),

  /** Explicitly denies access to the specified actions */
  DENY("Deny");

  private final String value;

  Effect(String value) {
    this.value = value;
  }

  /**
   * Returns the string representation used in policy documents.
   *
   * @return the effect value
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
