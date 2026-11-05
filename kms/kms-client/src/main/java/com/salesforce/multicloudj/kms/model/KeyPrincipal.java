package com.salesforce.multicloudj.kms.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/** Identity that a {@link KeyGrant} applies to. */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class KeyPrincipal {
  @NonNull private final PrincipalType type;

  /** Native identifier of the identity, interpreted according to {@link #getType()}. */
  @NonNull private final String identifier;

  public static KeyPrincipal of(PrincipalType type, String identifier) {
    return new KeyPrincipal(type, identifier);
  }
}
