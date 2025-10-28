package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single statement within a policy document.
 *
 * <p>A statement defines the permissions, principals, resources, and conditions
 * for a specific set of actions in a substrate-neutral format.
 *
 * <p>Usage example:
 * <pre>
 * Statement statement = Statement.builder()
 *     .sid("StorageAccess")
 *     .effect("Allow")
 *     .addAction("storage:GetObject")
 *     .addAction("storage:PutObject")
 *     .addPrincipal("arn:aws:iam::123456789012:user/ExampleUser")
 *     .addResource("storage://my-bucket/*")
 *     .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
 *     .build();
 * </pre>
 */
@Getter
public class Statement {
    private final String sid;
    private final String effect;
    private final List<String> principals;
    private final List<String> actions;
    private final List<String> resources;
    private final Map<String, Map<String, Object>> conditions;

    private Statement(Builder builder) {
        this.sid = builder.sid;
        this.effect = builder.effect;
        this.principals = new ArrayList<>(builder.principals);
        this.actions = new ArrayList<>(builder.actions);
        this.resources = new ArrayList<>(builder.resources);
        this.conditions = new HashMap<>(builder.conditions);
    }

    /**
     * Creates a new builder for Statement.
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
        Statement statement = (Statement) o;
        return Objects.equals(sid, statement.sid) &&
               Objects.equals(effect, statement.effect) &&
               Objects.equals(principals, statement.principals) &&
               Objects.equals(actions, statement.actions) &&
               Objects.equals(resources, statement.resources) &&
               Objects.equals(conditions, statement.conditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sid, effect, principals, actions, resources, conditions);
    }

    @Override
    public String toString() {
        return "Statement{" +
                "sid='" + sid + '\'' +
                ", effect='" + effect + '\'' +
                ", principals=" + principals +
                ", actions=" + actions +
                ", resources=" + resources +
                ", conditions=" + conditions +
                '}';
    }

    /**
     * Builder class for Statement.
     */
    public static class Builder {
        private String sid;
        private String effect;
        private final List<String> principals = new ArrayList<>();
        private final List<String> actions = new ArrayList<>();
        private final List<String> resources = new ArrayList<>();
        private final Map<String, Map<String, Object>> conditions = new HashMap<>();

        private Builder() {
        }

        /**
         * Sets the statement ID.
         *
         * @param sid the unique identifier for this statement within the policy
         * @return this Builder instance
         */
        public Builder sid(String sid) {
            this.sid = sid;
            return this;
        }

        /**
         * Sets the effect.
         *
         * @param effect "Allow" or "Deny"
         * @return this Builder instance
         */
        public Builder effect(String effect) {
            this.effect = effect;
            return this;
        }

        /**
         * Adds a principal to the statement.
         *
         * @param principal the principal (fully qualified principal required)
         * @return this Builder instance
         */
        public Builder addPrincipal(String principal) {
            if (principal != null && !principal.trim().isEmpty()) {
                this.principals.add(principal);
            }
            return this;
        }

        /**
         * Adds multiple principals to the statement.
         *
         * @param principals the list of principals
         * @return this Builder instance
         */
        public Builder addPrincipals(List<String> principals) {
            if (principals != null) {
                principals.stream()
                    .filter(p -> p != null && !p.trim().isEmpty())
                    .forEach(this.principals::add);
            }
            return this;
        }

        /**
         * Adds an action to the statement.
         *
         * @param action the action in substrate-neutral format (e.g., "storage:GetObject")
         * @return this Builder instance
         */
        public Builder addAction(String action) {
            if (action != null && !action.trim().isEmpty()) {
                this.actions.add(action);
            }
            return this;
        }

        /**
         * Adds multiple actions to the statement.
         *
         * @param actions the list of actions
         * @return this Builder instance
         */
        public Builder addActions(List<String> actions) {
            if (actions != null) {
                actions.stream()
                    .filter(a -> a != null && !a.trim().isEmpty())
                    .forEach(this.actions::add);
            }
            return this;
        }

        /**
         * Adds a resource to the statement.
         *
         * @param resource the resource in URI format (e.g., "storage://my-bucket/*")
         * @return this Builder instance
         */
        public Builder addResource(String resource) {
            if (resource != null && !resource.trim().isEmpty()) {
                this.resources.add(resource);
            }
            return this;
        }

        /**
         * Adds multiple resources to the statement.
         *
         * @param resources the list of resources
         * @return this Builder instance
         */
        public Builder addResources(List<String> resources) {
            if (resources != null) {
                resources.stream()
                    .filter(r -> r != null && !r.trim().isEmpty())
                    .forEach(this.resources::add);
            }
            return this;
        }

        /**
         * Adds a condition to the statement.
         *
         * @param operator the condition operator (e.g., "StringEquals", "IpAddress")
         * @param key the condition key (e.g., "aws:RequestedRegion")
         * @param value the condition value
         * @return this Builder instance
         */
        public Builder addCondition(String operator, String key, Object value) {
            if (operator != null && key != null && value != null) {
                conditions.computeIfAbsent(operator, k -> new HashMap<>()).put(key, value);
            }
            return this;
        }

        /**
         * Checks if the statement has the minimum required content to be built.
         *
         * @return true if the statement has both effect and at least one action
         */
        public boolean hasMinimumContent() {
            return effect != null && !effect.trim().isEmpty() && !actions.isEmpty();
        }

        /**
         * Builds and returns a Statement instance.
         *
         * @return a new Statement instance
         * @throws InvalidArgumentException if required fields are missing
         */
        public Statement build() {
            if (effect == null || effect.trim().isEmpty()) {
                throw new InvalidArgumentException("effect is required");
            }
            if (actions.isEmpty()) {
                throw new InvalidArgumentException("at least one action is required");
            }
            return new Statement(this);
        }
    }
}