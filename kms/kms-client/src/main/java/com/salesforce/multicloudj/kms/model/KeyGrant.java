package com.salesforce.multicloudj.kms.model;

import java.util.List;
import java.util.Set;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

/**
 * One rule of a {@link KeyPolicy}: {@link #getEffect()} the {@link #getActions()} on the key for
 * the {@link #getPrincipals()}, when every one of the {@link #getConditions()} holds.
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class KeyGrant {
  @Singular private final List<KeyPrincipal> principals;

  @Singular private final Set<KeyAction> actions;

  @Builder.Default private final Effect effect = Effect.ALLOW;

  @Singular private final List<KeyCondition> conditions;
}
