package com.salesforce.multicloudj.iam.model;

/**
 * Pre-defined compute service actions for IAM policies.
 *
 * <p>These constants represent common compute operations that can be used in policy statements
 * across AWS EC2, GCP Compute Engine, and other cloud providers.
 */
public final class ComputeActions {
  private ComputeActions() {
    // Prevent instantiation
  }

  /** Action to create/launch a new compute instance */
  public static final Action CREATE_INSTANCE = Action.of("compute:CreateInstance");

  /** Action to delete/terminate a compute instance */
  public static final Action DELETE_INSTANCE = Action.of("compute:DeleteInstance");

  /** Action to start a stopped compute instance */
  public static final Action START_INSTANCE = Action.of("compute:StartInstance");

  /** Action to stop a running compute instance */
  public static final Action STOP_INSTANCE = Action.of("compute:StopInstance");

  /** Action to list/describe compute instances */
  public static final Action DESCRIBE_INSTANCES = Action.of("compute:DescribeInstances");

  /** Action to get details of a specific compute instance */
  public static final Action GET_INSTANCE = Action.of("compute:GetInstance");

  /** Wildcard action representing all compute operations */
  public static final Action ALL = Action.wildcard("compute");
}
