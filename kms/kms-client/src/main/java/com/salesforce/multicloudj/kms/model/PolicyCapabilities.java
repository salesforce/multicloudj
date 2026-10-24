package com.salesforce.multicloudj.kms.model;

import java.util.EnumSet;
import java.util.Set;
import lombok.Builder;
import lombok.Getter;

/**
 * The subset of the {@link KeyPolicy} model a provider can enforce natively. A policy that needs
 * anything outside this subset is rejected rather than applied partially, because a partially
 * applied policy grants more access than the caller asked for.
 */
@Getter
@Builder
public class PolicyCapabilities {
  private final boolean supportsDeny;
  private final boolean supportsConditions;

  @Builder.Default
  private final Set<PrincipalType> supportedPrincipalTypes = EnumSet.allOf(PrincipalType.class);

  @Builder.Default private final Set<KeyAction> supportedActions = EnumSet.allOf(KeyAction.class);
}
