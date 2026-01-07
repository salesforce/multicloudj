package com.salesforce.multicloudj.sts.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Cloud-agnostic representation of credential scope restrictions for downscoped credentials.
 * This defines restrictions on what resources can be accessed and what permissions are available.
 * Maps to AccessBoundary in GCP and Policy in AWS.
 *
 * <p>Usage example with cloud-agnostic format:
 * <pre>
 * CredentialScope scope = CredentialScope.newBuilder()
 *     .addRule(CredentialScope.ScopeRule.newBuilder()
 *         .withAvailableResource("storage://my-bucket/*")
 *         .addAvailablePermission("storage:GetObject")
 *         .addAvailablePermission("storage:PutObject")
 *         .withAvailabilityCondition(CredentialScope.AvailabilityCondition.newBuilder()
 *             .withExpression("resource.name.startsWith('storage://my-bucket/prefix/')")
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
public class CredentialScope {

    private final List<ScopeRule> rules;

    private CredentialScope(Builder builder) {
        this.rules = new ArrayList<>(builder.rules);
    }

    public List<ScopeRule> getRules() {
        return new ArrayList<>(rules);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final List<ScopeRule> rules = new ArrayList<>();

        public Builder() {
        }

        public Builder addRule(ScopeRule rule) {
            this.rules.add(rule);
            return this;
        }

        public Builder addAllRules(List<ScopeRule> rules) {
            this.rules.addAll(rules);
            return this;
        }

        public CredentialScope build() {
            return new CredentialScope(this);
        }
    }

    /**
     * Represents a single rule in a credential scope.
     */
    public static class ScopeRule {
        @Getter
        private final String availableResource;
        private final List<String> availablePermissions;
        @Getter
        private final AvailabilityCondition availabilityCondition;

        private ScopeRule(Builder builder) {
            this.availableResource = builder.availableResource;
            this.availablePermissions = new ArrayList<>(builder.availablePermissions);
            this.availabilityCondition = builder.availabilityCondition;
        }

        public List<String> getAvailablePermissions() {
            return new ArrayList<>(availablePermissions);
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private String availableResource;
            private final List<String> availablePermissions = new ArrayList<>();
            private AvailabilityCondition availabilityCondition;

            public Builder() {
            }

            public Builder withAvailableResource(String availableResource) {
                this.availableResource = availableResource;
                return this;
            }

            public Builder addAvailablePermission(String permission) {
                this.availablePermissions.add(permission);
                return this;
            }

            public Builder addAllAvailablePermissions(List<String> permissions) {
                this.availablePermissions.addAll(permissions);
                return this;
            }

            public Builder withAvailabilityCondition(AvailabilityCondition condition) {
                this.availabilityCondition = condition;
                return this;
            }

            public ScopeRule build() {
                return new ScopeRule(this);
            }
        }
    }

    /**
     * Represents a condition that must be met for a rule to apply.
     */
    @Getter
    public static class AvailabilityCondition {
        private final String expression;
        private final String title;
        private final String description;

        private AvailabilityCondition(Builder builder) {
            this.expression = builder.expression;
            this.title = builder.title;
            this.description = builder.description;
        }

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private String expression;
            private String title;
            private String description;

            public Builder() {
            }

            public Builder withExpression(String expression) {
                this.expression = expression;
                return this;
            }

            public Builder withTitle(String title) {
                this.title = title;
                return this;
            }

            public Builder withDescription(String description) {
                this.description = description;
                return this;
            }

            public AvailabilityCondition build() {
                return new AvailabilityCondition(this);
            }
        }
    }
}
