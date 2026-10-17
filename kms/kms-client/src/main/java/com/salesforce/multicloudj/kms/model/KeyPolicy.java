package com.salesforce.multicloudj.kms.model;

import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;

/**
 * Access policy attached to a key. Setting a policy replaces the key's existing policy as a whole.
 */
@Getter
@Builder
@EqualsAndHashCode
@ToString
public class KeyPolicy {
  @Singular private final List<KeyGrant> grants;
}
