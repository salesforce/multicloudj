package com.salesforce.multicloudj.iam.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a substrate-neutral policy document containing multiple statements.
 *
 * <p>This class provides a cloud-agnostic way to define IAM policies that can be
 * translated to AWS, GCP, or AliCloud native formats. The policy uses a builder
 * pattern to prevent JSON parsing errors and provides type safety.
 *
 * <p>Usage example:
 * <pre>
 * PolicyDocument policy = PolicyDocument.builder()
 *     .version("2024-01-01")
 *     .statement("StorageAccess")
 *         .effect("Allow")
 *         .addAction("storage:GetObject")
 *         .addAction("storage:PutObject")
 *         .addPrincipal("arn:aws:iam::123456789012:user/ExampleUser")
 *         .addResource("storage://my-bucket/*")
 *         .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
 *     .endStatement()
 *     .build();
 * </pre>
 */
@Getter
public class PolicyDocument {
    private final String version;
    private final List<Statement> statements;

    private PolicyDocument(Builder builder) {
        this.version = builder.version;
        this.statements = new ArrayList<>(builder.statements);
    }

    /**
     * Creates a new builder for PolicyDocument.
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
        PolicyDocument that = (PolicyDocument) o;
        return Objects.equals(version, that.version) &&
               Objects.equals(statements, that.statements);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, statements);
    }

    @Override
    public String toString() {
        return "PolicyDocument{" +
                "version='" + version + '\'' +
                ", statements=" + statements +
                '}';
    }

    /**
     * Builder class for PolicyDocument.
     */
    public static class Builder {
        private String version = "2024-01-01";
        private final List<Statement> statements = new ArrayList<>();
        private Statement.Builder currentStatementBuilder;

        private Builder() {
        }

        /**
         * Sets the policy version.
         *
         * @param version the policy version (default: "2024-01-01")
         * @return this Builder instance
         */
        public Builder version(String version) {
            this.version = version;
            return this;
        }

        /**
         * Starts building a new statement with the given SID.
         *
         * @param sid the statement ID
         * @return this Builder instance configured for statement building
         */
        public Builder statement(String sid) {
            finalizeCurrentStatement();
            this.currentStatementBuilder = Statement.builder().sid(sid);
            return this;
        }

        /**
         * Sets the effect for the current statement.
         *
         * @param effect "Allow" or "Deny"
         * @return this Builder instance
         */
        public Builder effect(String effect) {
            validateCurrentStatement();
            this.currentStatementBuilder.effect(effect);
            return this;
        }

        /**
         * Adds a principal to the current statement.
         *
         * @param principal the principal (fully qualified principal required)
         * @return this Builder instance
         */
        public Builder addPrincipal(String principal) {
            validateCurrentStatement();
            this.currentStatementBuilder.addPrincipal(principal);
            return this;
        }

        /**
         * Adds multiple principals to the current statement.
         *
         * @param principals the list of principals
         * @return this Builder instance
         */
        public Builder addPrincipals(List<String> principals) {
            validateCurrentStatement();
            this.currentStatementBuilder.addPrincipals(principals);
            return this;
        }

        /**
         * Adds an action to the current statement.
         *
         * @param action the action in substrate-neutral format
         * @return this Builder instance
         */
        public Builder addAction(String action) {
            validateCurrentStatement();
            this.currentStatementBuilder.addAction(action);
            return this;
        }

        /**
         * Adds multiple actions to the current statement.
         *
         * @param actions the list of actions
         * @return this Builder instance
         */
        public Builder addActions(List<String> actions) {
            validateCurrentStatement();
            this.currentStatementBuilder.addActions(actions);
            return this;
        }

        /**
         * Adds a resource to the current statement.
         *
         * @param resource the resource in URI format
         * @return this Builder instance
         */
        public Builder addResource(String resource) {
            validateCurrentStatement();
            this.currentStatementBuilder.addResource(resource);
            return this;
        }

        /**
         * Adds multiple resources to the current statement.
         *
         * @param resources the list of resources
         * @return this Builder instance
         */
        public Builder addResources(List<String> resources) {
            validateCurrentStatement();
            this.currentStatementBuilder.addResources(resources);
            return this;
        }

        /**
         * Adds a condition to the current statement.
         *
         * @param operator the condition operator
         * @param key the condition key
         * @param value the condition value
         * @return this Builder instance
         */
        public Builder addCondition(String operator, String key, Object value) {
            validateCurrentStatement();
            this.currentStatementBuilder.addCondition(operator, key, value);
            return this;
        }

        /**
         * Ends the current statement and adds it to the policy.
         *
         * @return this Builder instance
         */
        public Builder endStatement() {
            finalizeCurrentStatement();
            return this;
        }

        /**
         * Adds a complete statement to the policy document.
         *
         * @param statement the statement to add
         * @return this Builder instance
         */
        public Builder addStatement(Statement statement) {
            finalizeCurrentStatement();
            if (statement != null) {
                this.statements.add(statement);
            }
            return this;
        }

        /**
         * Builds and returns a PolicyDocument instance.
         *
         * @return a new PolicyDocument instance
         * @throws IllegalArgumentException if no statements are defined
         */
        public PolicyDocument build() {
            finalizeCurrentStatement();
            if (statements.isEmpty()) {
                throw new IllegalArgumentException("at least one statement is required");
            }
            return new PolicyDocument(this);
        }

        private void validateCurrentStatement() {
            if (currentStatementBuilder == null) {
                throw new IllegalStateException("No statement is currently being built. Call statement(sid) first.");
            }
        }

        private void finalizeCurrentStatement() {
            if (currentStatementBuilder != null) {
                statements.add(currentStatementBuilder.build());
                currentStatementBuilder = null;
            }
        }
    }
}