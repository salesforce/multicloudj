package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

/**
 * Represents a single statement within a policy document.
 *
 * <p>A statement defines the permissions, principals, resources, and conditions for a specific set
 * of actions in a substrate-neutral format.
 *
 * <p>Usage example:
 *
 * <pre>
 * Statement statement = Statement.builder()
 *     .sid("StorageAccess")
 *     .effect(Effect.ALLOW)
 *     .action(StorageActions.GET_OBJECT)
 *     .action(StorageActions.PUT_OBJECT)
 *     .principal("arn:aws:iam::123456789012:user/ExampleUser")
 *     .resource("storage://my-bucket/*")
 *     .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
 *     .build();
 * </pre>
 */
@Getter
public class Statement {
  private final String sid;
  private final Effect effect;
  private final List<String> principals;
  private final List<Action> actions;
  private final List<String> resources;
  private final Map<ConditionOperator, Map<String, Object>> conditions;

  @Builder
  private Statement(
      String sid,
      Effect effect,
      @Singular List<String> principals,
      @Singular List<Action> actions,
      @Singular List<String> resources,
      Map<ConditionOperator, Map<String, Object>> conditions) {

    // Validate effect
    if (effect == null) {
      throw new InvalidArgumentException("Effect is required and cannot be null");
    }

    // Filter out null/empty/whitespace values
    this.sid = sid;
    this.effect = effect;
    this.principals = filterValidStrings(principals);
    this.resources = filterValidStrings(resources);

    // Filter and validate actions
    if (actions == null || actions.isEmpty()) {
      throw new InvalidArgumentException("At least one action is required");
    }
    this.actions = actions.stream().filter(Objects::nonNull).collect(Collectors.toList());

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
        .collect(Collectors.toList());
  }

  /**
   * Returns actions as strings for internal use by translators.
   *
   * @return list of action strings in format "service:operation"
   */
  public List<String> getActionsAsStrings() {
    return actions.stream().map(Action::toActionString).collect(Collectors.toList());
  }

  /**
   * Returns conditions with string keys for internal use by translators.
   *
   * @return map of condition operator strings to condition maps
   */
  public Map<String, Map<String, Object>> getConditionsAsStrings() {
    return conditions.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getValue(), Map.Entry::getValue));
  }

  /** Custom builder for Statement to handle conditions. */
  public static class StatementBuilder {
    /**
     * Adds a condition to the statement.
     *
     * @param operator the condition operator
     * @param key the condition key
     * @param value the condition value
     * @return this builder
     */
    public StatementBuilder condition(ConditionOperator operator, String key, Object value) {
      if (operator != null && key != null && value != null) {
        if (this.conditions == null) {
          this.conditions = new java.util.HashMap<>();
        }
        this.conditions.computeIfAbsent(operator, k -> new java.util.HashMap<>()).put(key, value);
      }
      return this;
    }
  }
}
