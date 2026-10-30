package com.salesforce.multicloudj.kms.model;

import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;

/**
 * Restriction on when a {@link KeyGrant} applies: the request attribute identified by {@link
 * #getAttribute()} (and {@link #getKey()} where the attribute needs one) must satisfy {@link
 * #getOperator()} against at least one of {@link #getValues()}.
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class KeyCondition {
  @NonNull private final ConditionOperator operator;

  @NonNull private final ConditionAttribute attribute;

  /**
   * Encryption-context entry name for {@link ConditionAttribute#ENCRYPTION_CONTEXT}, or the native
   * attribute name for {@link ConditionAttribute#NATIVE}; unset for the other attributes.
   */
  private final String key;

  @Singular private final List<String> values;
}
