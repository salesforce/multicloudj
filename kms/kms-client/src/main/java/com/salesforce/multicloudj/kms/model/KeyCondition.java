package com.salesforce.multicloudj.kms.model;

import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;

/**
 * Restriction on when a {@link KeyGrant} applies: the request attribute named by {@link #getKey()}
 * must satisfy {@link #getOperator()} against at least one of {@link #getValues()}.
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class KeyCondition {
  @NonNull private final ConditionOperator operator;

  /** Native name of the request attribute the condition evaluates. */
  @NonNull private final String key;

  @Singular private final List<String> values;
}
