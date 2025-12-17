package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

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

  /**
   * Custom builder for Statement to handle conditions.
   */
  public static class StatementBuilder {
    /**
     * Adds a condition to the statement.
     *
     * @param operator the condition operator
     * @param key the condition key
     * @param value the condition value
     * @return this builder
     */
    public StatementBuilder condition(String operator, String key, Object value) {
      if (operator != null && key != null && value != null) {
        if (this.conditions == null) {
          this.conditions = new java.util.HashMap<>();
        }
        this.conditions.computeIfAbsent(operator, k -> new java.util.HashMap<>())
                .put(key, value);
      }
      return this;
    }
  }
}