package com.salesforce.multicloudj.iam.model;

/**
 * Pre-defined IAM service actions for IAM policies.
 *
 * <p>These constants represent common IAM operations that can be used in policy statements across
 * AWS IAM, GCP IAM, and other cloud providers.
 */
public final class IamActions {
  private IamActions() {
    // Prevent instantiation
  }

  /** Action to assume a role (for cross-account or service access) */
  public static final Action ASSUME_ROLE = Action.of("iam:AssumeRole");

  /** Action to create a new IAM role */
  public static final Action CREATE_ROLE = Action.of("iam:CreateRole");

  /** Action to delete an IAM role */
  public static final Action DELETE_ROLE = Action.of("iam:DeleteRole");

  /** Action to get details of an IAM role */
  public static final Action GET_ROLE = Action.of("iam:GetRole");

  /** Action to attach a policy to a role */
  public static final Action ATTACH_ROLE_POLICY = Action.of("iam:AttachRolePolicy");

  /** Action to detach a policy from a role */
  public static final Action DETACH_ROLE_POLICY = Action.of("iam:DetachRolePolicy");

  /** Action to create/update an inline role policy */
  public static final Action PUT_ROLE_POLICY = Action.of("iam:PutRolePolicy");

  /** Action to get an inline role policy */
  public static final Action GET_ROLE_POLICY = Action.of("iam:GetRolePolicy");

  /** Wildcard action representing all IAM operations */
  public static final Action ALL = Action.wildcard("iam");
}
