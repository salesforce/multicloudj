package com.salesforce.multicloudj.kms.model;

/** Kind of identity a {@link KeyPrincipal} identifies. */
public enum PrincipalType {
  /** A role or workload identity, identified by its native identifier. */
  IAM_ROLE,

  /** A cloud service acting on its own behalf, identified by its native service name. */
  SERVICE_ROLE,

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
