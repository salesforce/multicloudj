package com.salesforce.multicloudj.iam.model;

import lombok.Builder;
import lombok.Getter;

import java.util.Objects;

/**
 * Optional configuration for identity creation operations.
 *
 * <p>This class provides additional options that can be set during identity creation,
 * such as path specifications, session duration limits, and permission boundaries.
 *
 * <p>Permission boundary identifiers are provider-specific and translated internally:
 * - AWS: IAM Policy ARN format (arn:aws:iam::account:policy/name)
 * - GCP: Organization Policy constraint name or IAM Condition expression
 * - AliCloud: Control Policy name or ID (Resource Directory Control Policies)
 *
 * <p>Usage examples by provider:
 * <pre>
 * // AWS Example
 * CreateOptions awsOptions = CreateOptions.builder()
 *     .path("/foo/")
 *     .maxSessionDuration(43200) // 12 hours
 *     .permissionBoundary("arn:aws:iam::123456789012:policy/PowerUserBoundary")
 *     .build();
 *
 * // GCP Example (using organization policy constraint)
 * CreateOptions gcpOptions = CreateOptions.builder()
 *     .path("/foo/")
 *     .maxSessionDuration(3600)  // 1 hour
 *     .permissionBoundary("constraints/compute.restrictLoadBalancerCreationForTypes")
 *     .build();
 *
 * // AliCloud Example (using Control Policy)
 * CreateOptions aliOptions = CreateOptions.builder()
 *     .path("/foo/")
 *     .maxSessionDuration(7200)  // 2 hours
 *     .permissionBoundary("cp-bp1example") // Control Policy ID
 *     .build();
 * </pre>
 */
@Getter
@Builder
public class CreateOptions {
    private final String path;
    private final Integer maxSessionDuration;
    private final String permissionBoundary;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateOptions that = (CreateOptions) o;
        return Objects.equals(path, that.path) &&
               Objects.equals(maxSessionDuration, that.maxSessionDuration) &&
               Objects.equals(permissionBoundary, that.permissionBoundary);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, maxSessionDuration, permissionBoundary);
    }

    @Override
    public String toString() {
        return "CreateOptions{" +
                "path='" + path + '\'' +
                ", maxSessionDuration=" + maxSessionDuration +
                ", permissionBoundary='" + permissionBoundary + '\'' +
                '}';
    }
}