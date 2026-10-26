package com.salesforce.multicloudj.kms.model;

/** Kind of identity a {@link KeyPrincipal} identifies. */
public enum PrincipalType {
  /**
   * A single identity, such as a role, service account or user. The identifier is the identity's
   * native identifier as returned by the IAM client's {@code getIdentity}, passed through
   * unchanged.
   */
  IDENTITY,

  /** Every identity in an account or project, identified by the account or project id. */
  ACCOUNT,

  /** Every identity in an organization, identified by the organization id. */
  TENANT,

  /**
   * Every identity whose native identifier matches a pattern; the identifier is the pattern, which
   * may contain {@code *} and {@code ?} wildcards.
   */
  WILDCARD
}
