package com.salesforce.multicloudj.iam.gcp;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.Action;
import com.salesforce.multicloudj.iam.model.ComputeActions;
import com.salesforce.multicloudj.iam.model.IamActions;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.StorageActions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Translates substrate-neutral PolicyDocument actions to GCP IAM roles.
 *
 * <p>This translator converts substrate-neutral actions to GCP-specific IAM roles according to the
 * translation rules defined in PolicyDocument documentation.
 *
 * <p>Translation rules:
 *
 * <ul>
 *   <li>Actions: storage:GetObject → roles/storage.objectViewer
 *   <li>Actions: storage:PutObject → roles/storage.objectCreator
 *   <li>Actions: compute:CreateInstance → roles/compute.instanceAdmin.v1
 *   <li>Actions: iam:AssumeRole → roles/iam.serviceAccountUser
 * </ul>
 *
 * <p>Note: GCP IAM uses role-based access control, not action-based like AWS. Each
 * substrate-neutral action maps to a GCP predefined role. Multiple actions may result in multiple
 * role bindings.
 */
public class GcpIamPolicyTranslator {

  // Action mappings: substrate-neutral → GCP role
  private static final Map<Action, String> ACTION_TO_ROLE_MAPPINGS =
      Map.ofEntries(
          // Storage actions
          Map.entry(StorageActions.GET_OBJECT, "roles/storage.objectViewer"),
          Map.entry(StorageActions.PUT_OBJECT, "roles/storage.objectCreator"),
          Map.entry(StorageActions.DELETE_OBJECT, "roles/storage.objectAdmin"),
          Map.entry(StorageActions.LIST_BUCKET, "roles/storage.objectViewer"),
          Map.entry(StorageActions.GET_BUCKET_LOCATION, "roles/storage.objectViewer"),
          Map.entry(StorageActions.CREATE_BUCKET, "roles/storage.admin"),
          Map.entry(StorageActions.DELETE_BUCKET, "roles/storage.admin"),

          // Compute actions
          Map.entry(ComputeActions.CREATE_INSTANCE, "roles/compute.instanceAdmin.v1"),
          Map.entry(ComputeActions.DELETE_INSTANCE, "roles/compute.instanceAdmin.v1"),
          Map.entry(ComputeActions.START_INSTANCE, "roles/compute.instanceAdmin.v1"),
          Map.entry(ComputeActions.STOP_INSTANCE, "roles/compute.instanceAdmin.v1"),
          Map.entry(ComputeActions.DESCRIBE_INSTANCES, "roles/compute.viewer"),
          Map.entry(ComputeActions.GET_INSTANCE, "roles/compute.viewer"),

          // IAM actions
          Map.entry(IamActions.ASSUME_ROLE, "roles/iam.serviceAccountUser"),
          Map.entry(IamActions.CREATE_ROLE, "roles/iam.serviceAccountAdmin"),
          Map.entry(IamActions.DELETE_ROLE, "roles/iam.serviceAccountAdmin"),
          Map.entry(IamActions.GET_ROLE, "roles/iam.serviceAccountViewer"),
          Map.entry(IamActions.ATTACH_ROLE_POLICY, "roles/iam.serviceAccountAdmin"),
          Map.entry(IamActions.DETACH_ROLE_POLICY, "roles/iam.serviceAccountAdmin"),
          Map.entry(IamActions.PUT_ROLE_POLICY, "roles/iam.serviceAccountAdmin"),
          Map.entry(IamActions.GET_ROLE_POLICY, "roles/iam.serviceAccountViewer"));

  /**
   * Translates substrate-neutral actions from a statement to GCP IAM roles.
   *
   * @param statement the substrate-neutral statement
   * @return list of GCP IAM roles
   * @throws SubstrateSdkException if action is unknown or conditions are unsupported
   */
  public static List<String> translateActionsToRoles(Statement statement) {
    if (statement.getEffect() == null) {
      throw new InvalidArgumentException("Effect is required for GCP IAM policy statement");
    }

    // Check for unsupported conditions
    if (statement.getConditions() != null && !statement.getConditions().isEmpty()) {
      // GCP IAM v1 bindings support limited conditions via CEL expressions
      // For now, we throw an error for any conditions as basic implementation
      throw new InvalidArgumentException(
          "GCP IAM policy conditions are not yet supported in substrate-neutral translation. "
              + "Statement SID: "
              + (statement.getSid() != null ? statement.getSid() : "unnamed")
              + ". "
              + "GCP requires IAM Conditions API (v2) with CEL expressions.");
    }

    List<String> roles = new ArrayList<>();
    for (Action action : statement.getActions()) {
      String role = translateActionToRole(action);
      if (!roles.contains(role)) {
        roles.add(role);
      }
    }
    return roles;
  }

  /**
   * Translates a single substrate-neutral action to a GCP IAM role. Supports wildcard actions like
   * storage:*, compute:*, iam:*.
   *
   * @param action the substrate-neutral action
   * @return GCP IAM role
   * @throws SubstrateSdkException if action is unknown
   */
  public static String translateActionToRole(Action action) {
    // Handle wildcard actions (e.g., storage:*, compute:*, iam:*)
    if (action.isWildcard()) {
      String service = action.getService();
      switch (service) {
        case "storage":
          // For storage:*, grant the most comprehensive storage role
          return "roles/storage.admin";
        case "compute":
          // For compute:*, grant the most comprehensive compute role
          return "roles/compute.admin";
        case "iam":
          // For iam:*, grant the most comprehensive IAM role
          return "roles/iam.serviceAccountAdmin";
        default:
          throw new InvalidArgumentException(
              "Unknown substrate-neutral service for wildcard action: "
                  + action.toActionString()
                  + ". "
                  + "Supported wildcard services: storage:*, compute:*, iam:*");
      }
    }

    // Handle specific actions
    String role = ACTION_TO_ROLE_MAPPINGS.get(action);
    if (role == null) {
      throw new InvalidArgumentException(
          "Unknown substrate-neutral action: "
              + action.toActionString()
              + ". "
              + "Supported actions: "
              + ACTION_TO_ROLE_MAPPINGS.keySet().stream()
                  .map(Action::toActionString)
                  .collect(Collectors.joining(", "))
              + ", or wildcard actions: storage:*, compute:*, iam:*");
    }
    return role;
  }
}
