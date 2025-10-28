package com.salesforce.multicloudj.iam.model;

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
 * - AliCloud: Not supported (AliCloud RAM does not have permission boundaries)
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
 * // AliCloud Example (permission boundaries not supported)
 * CreateOptions aliOptions = CreateOptions.builder()
 *     .path("/foo/")
 *     .maxSessionDuration(7200)  // 2 hours
 *     // .permissionBoundary() - Not supported in AliCloud RAM
 *     .build();
 * </pre>
 */
@Getter
public class CreateOptions {
    private final String path;
    private final Integer maxSessionDuration;
    private final String permissionBoundary;

    private CreateOptions(Builder builder) {
        this.path = builder.path;
        this.maxSessionDuration = builder.maxSessionDuration;
        this.permissionBoundary = builder.permissionBoundary;
    }

    /**
     * Creates a new builder for CreateOptions.
     *
     * @return a new Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }


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

    /**
     * Builder class for CreateOptions.
     */
    public static class Builder {
        private String path;
        private Integer maxSessionDuration;
        private String permissionBoundary;

        private Builder() {
        }

        /**
         * Sets the path for the identity.
         *
         * @param path the path (e.g., "/foo/") for organizing identities
         * @return this Builder instance
         */
        public Builder path(String path) {
            this.path = path;
            return this;
        }

        /**
         * Sets the maximum session duration in seconds.
         *
         * @param maxSessionDuration the maximum session duration (typically up to 12 hours = 43200 seconds)
         * @return this Builder instance
         */
        public Builder maxSessionDuration(Integer maxSessionDuration) {
            this.maxSessionDuration = maxSessionDuration;
            return this;
        }

        /**
         * Sets the permission boundary policy identifier.
         *
         * @param permissionBoundary the cloud-native identifier of the policy that acts as a permission boundary
         *                          (AWS: policy ARN, GCP: constraint name, AliCloud: not supported)
         * @return this Builder instance
         */
        public Builder permissionBoundary(String permissionBoundary) {
            this.permissionBoundary = permissionBoundary;
            return this;
        }

        /**
         * Builds and returns a CreateOptions instance.
         *
         * @return a new CreateOptions instance
         */
        public CreateOptions build() {
            return new CreateOptions(this);
        }
    }
}