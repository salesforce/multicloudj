package com.salesforce.multicloudj.iam.model;

import lombok.Getter;

import java.util.Objects;

/**
 * Optional configuration for identity creation operations.
 *
 * <p>This class provides additional options that can be set during identity creation,
 * such as path specifications, session duration limits, and permission boundaries.
 *
 * <p>Usage example:
 * <pre>
 * CreateOptions options = CreateOptions.builder()
 *     .path("/foo/")
 *     .maxSessionDuration(43200) // 12 hours
 *     .permissionBoundary("arn:aws:iam::123456789012:policy/PowerUserBoundary")
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
         * Sets the permission boundary ARN.
         *
         * @param permissionBoundary the ARN of the policy that acts as a permissions boundary
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