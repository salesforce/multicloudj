package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

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
 *     .action("storage:GetObject")
 *     .action("storage:PutObject")
 *     .principal("arn:aws:iam::123456789012:user/ExampleUser")
 *     .resource("storage://my-bucket/*")
 *     .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
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

    @Builder
    private Statement(String sid, String effect,
                     @Singular List<String> principals,
                     @Singular List<String> actions,
                     @Singular List<String> resources,
                     Map<String, Map<String, Object>> conditions) {
        // Validate effect
        if (effect == null || effect.trim().isEmpty()) {
            throw new InvalidArgumentException("Effect is required and cannot be empty");
        }

        // Filter out null/empty/whitespace values and validate actions
        this.sid = sid;
        this.effect = effect;
        this.principals = filterValidStrings(principals);
        this.actions = filterValidStrings(actions);
        this.resources = filterValidStrings(resources);

        // Validate that at least one action exists after filtering
        if (this.actions.isEmpty()) {
            throw new InvalidArgumentException("At least one action is required");
        }

        this.conditions = conditions != null ? conditions : new java.util.HashMap<>();
    }

    private static List<String> filterValidStrings(List<String> input) {
        if (input == null) {
            return new java.util.ArrayList<>();
        }
        return input.stream()
                .filter(s -> s != null && !s.trim().isEmpty())
                .collect(java.util.stream.Collectors.toList());
    }

    public static class StatementBuilder {
        public StatementBuilder condition(String operator, String key, Object value) {
            if (operator != null && key != null && value != null) {
                if (this.conditions == null) {
                    this.conditions = new java.util.HashMap<>();
                }
                this.conditions.computeIfAbsent(operator, k -> new java.util.HashMap<>()).put(key, value);
            }
            return this;
        }
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
}