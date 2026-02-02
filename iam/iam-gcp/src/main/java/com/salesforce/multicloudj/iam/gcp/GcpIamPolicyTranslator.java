package com.salesforce.multicloudj.iam.gcp;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Translates substrate-neutral PolicyDocument actions to GCP IAM roles.
 * 
 * <p>This translator converts substrate-neutral actions to GCP-specific IAM roles
 * according to the translation rules defined in PolicyDocument documentation.
 * 
 * <p>Translation rules:
 * <ul>
 *   <li>Actions: storage:GetObject → roles/storage.objectViewer</li>
 *   <li>Actions: storage:PutObject → roles/storage.objectCreator</li>
 *   <li>Actions: compute:CreateInstance → roles/compute.instanceAdmin.v1</li>
 *   <li>Actions: iam:AssumeRole → roles/iam.serviceAccountUser</li>
 * </ul>
 * 
 * <p>Note: GCP IAM uses role-based access control, not action-based like AWS.
 * Each substrate-neutral action maps to a GCP predefined role. Multiple actions
 * may result in multiple role bindings.
 */
public class GcpIamPolicyTranslator {

    // Action mappings: substrate-neutral → GCP role
    private static final Map<String, String> ACTION_TO_ROLE_MAPPINGS = Map.ofEntries(
        // Storage actions
        Map.entry("storage:GetObject", "roles/storage.objectViewer"),
        Map.entry("storage:PutObject", "roles/storage.objectCreator"),
        Map.entry("storage:DeleteObject", "roles/storage.objectAdmin"),
        Map.entry("storage:ListBucket", "roles/storage.objectViewer"),
        Map.entry("storage:GetBucketLocation", "roles/storage.objectViewer"),
        Map.entry("storage:CreateBucket", "roles/storage.admin"),
        Map.entry("storage:DeleteBucket", "roles/storage.admin"),
        
        // Compute actions
        Map.entry("compute:CreateInstance", "roles/compute.instanceAdmin.v1"),
        Map.entry("compute:DeleteInstance", "roles/compute.instanceAdmin.v1"),
        Map.entry("compute:StartInstance", "roles/compute.instanceAdmin.v1"),
        Map.entry("compute:StopInstance", "roles/compute.instanceAdmin.v1"),
        Map.entry("compute:DescribeInstances", "roles/compute.viewer"),
        Map.entry("compute:GetInstance", "roles/compute.viewer"),
        
        // IAM actions
        Map.entry("iam:AssumeRole", "roles/iam.serviceAccountUser"),
        Map.entry("iam:CreateRole", "roles/iam.serviceAccountAdmin"),
        Map.entry("iam:DeleteRole", "roles/iam.serviceAccountAdmin"),
        Map.entry("iam:GetRole", "roles/iam.serviceAccountViewer"),
        Map.entry("iam:AttachRolePolicy", "roles/iam.serviceAccountAdmin"),
        Map.entry("iam:DetachRolePolicy", "roles/iam.serviceAccountAdmin"),
        Map.entry("iam:PutRolePolicy", "roles/iam.serviceAccountAdmin"),
        Map.entry("iam:GetRolePolicy", "roles/iam.serviceAccountViewer")
    );

    /**
     * Translates substrate-neutral actions from a statement to GCP IAM roles.
     *
     * @param statement the substrate-neutral statement
     * @return list of GCP IAM roles
     * @throws SubstrateSdkException if action is unknown or conditions are unsupported
     */
    public static List<String> translateActionsToRoles(Statement statement) {
        // Check for unsupported conditions
        if (statement.getConditions() != null && !statement.getConditions().isEmpty()) {
            // GCP IAM v1 bindings support limited conditions via CEL expressions
            // For now, we throw an error for any conditions as basic implementation
            throw new SubstrateSdkException(
                "GCP IAM policy conditions are not yet supported in substrate-neutral translation. " +
                "Statement SID: " + (statement.getSid() != null ? statement.getSid() : "unnamed") + ". " +
                "GCP requires IAM Conditions API (v2) with CEL expressions."
            );
        }

        List<String> roles = new ArrayList<>();
        for (String action : statement.getActions()) {
            String role = translateActionToRole(action);
            if (!roles.contains(role)) {
                roles.add(role);
            }
        }
        return roles;
    }

    /**
     * Translates a single substrate-neutral action to a GCP IAM role.
     * Supports wildcard actions like storage:*, compute:*, iam:*.
     *
     * @param action the substrate-neutral action
     * @return GCP IAM role
     * @throws SubstrateSdkException if action is unknown
     */
    public static String translateActionToRole(String action) {
        // Handle wildcard actions (e.g., storage:*, compute:*, iam:*)
        if (action.endsWith(":*")) {
            String service = action.substring(0, action.length() - 2);
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
                    throw new SubstrateSdkException(
                        "Unknown substrate-neutral service for wildcard action: " + action + ". " +
                        "Supported wildcard services: storage:*, compute:*, iam:*"
                    );
            }
        }
        
        // Handle specific actions
        String role = ACTION_TO_ROLE_MAPPINGS.get(action);
        if (role == null) {
            throw new SubstrateSdkException(
                "Unknown substrate-neutral action: " + action + ". " +
                "Supported actions: " + String.join(", ", ACTION_TO_ROLE_MAPPINGS.keySet()) +
                ", or wildcard actions: storage:*, compute:*, iam:*"
            );
        }
        return role;
    }
}
