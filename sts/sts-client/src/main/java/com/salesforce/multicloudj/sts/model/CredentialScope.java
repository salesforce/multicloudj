package com.salesforce.multicloudj.sts.model;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.util.List;

/**
 * Cloud-agnostic representation of credential scope restrictions for downs-coped credentials based on
 * access boundary or policies.
 * This defines restrictions on what resources can be accessed and what permissions are available.
 * Maps to AccessBoundary in GCP and Policy in AWS.
 *
 * <p>Usage example with cloud-agnostic format:
 * <pre>
 * CredentialScope scope = CredentialScope.builder()
 *     .rule(CredentialScope.ScopeRule.builder()
 *         .availableResource("storage://my-bucket/*")
 *         .availablePermission("storage:GetObject")
 *         .availablePermission("storage:PutObject")
 *         .availabilityCondition(CredentialScope.AvailabilityCondition.builder()
 *             .expression("resource.name.startsWith('storage://my-bucket/prefix/')")
 *             .build())
 *         .build())
 *     .build();
 * </pre>
 *
 * <p>Use cloud-agnostic action formats:
 * <ul>
 *   <li>storage:GetObject - Read objects from storage</li>
 *   <li>storage:PutObject - Write objects to storage</li>
 *   <li>storage:DeleteObject - Delete objects from storage</li>
 *   <li>storage:ListBucket - List bucket contents</li>
 * </ul>
 *
 * <p>Use cloud-agnostic resource formats:
 * <ul>
 *   <li>storage://bucket-name/* - All objects in bucket</li>
 *   <li>storage://bucket-name/prefix/* - Objects under prefix</li>
 * </ul>
 */
@Getter
@Builder
public class CredentialScope {

    @Singular
    private final List<ScopeRule> rules;

    /**
     * Represents a single rule in a credential scope.
     */
    @Getter
    @Builder
    public static class ScopeRule {
        private final String availableResource;
        @Singular
        private final List<String> availablePermissions;
        private final AvailabilityCondition availabilityCondition;
    }

    /**
     * Represents a condition that must be met for a rule to apply.
     * Uses cloud-agnostic structured conditions.
     *
     * <p>Usage example:
     * <pre>
     * AvailabilityCondition.builder()
     *     .resourcePrefix("storage://my-bucket/documents/")
     *     .title("Limit to documents folder")
     *     .description("Only allow access to documents")
     *     .build()
     * </pre>
     */
    @Getter
    @Builder
    public static class AvailabilityCondition {
        /**
         * Cloud-agnostic resource prefix constraint.
         * Example: "storage://my-bucket/documents/" restricts access to objects under that prefix.
         * Automatically converted to provider-specific format (CEL for GCP, IAM Condition for AWS).
         */
        private final String resourcePrefix;

        /**
         * Optional title for the condition.
         */
        private final String title;

        /**
         * Optional description of what this condition restricts.
         */
        private final String description;
    }
}
