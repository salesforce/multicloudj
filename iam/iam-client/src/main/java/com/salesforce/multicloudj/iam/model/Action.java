package com.salesforce.multicloudj.iam.model;

import com.fasterxml.jackson.annotation.JsonValue;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.Objects;

/**
 * Represents a substrate-neutral IAM action.
 *
 * <p>Actions follow the format {@code service:operation}, where:
 *
 * <ul>
 *   <li>service: The cloud service (e.g., "storage", "compute", "iam")
 *   <li>operation: The operation name (e.g., "GetObject", "CreateInstance") or "*" for all
 * </ul>
 *
 * <p>Use pre-defined constants from {@link StorageActions}, {@link ComputeActions}, and {@link
 * IamActions} for common actions, or create custom actions using {@link Action#of(String)}.
 *
 * <p>Example usage:
 *
 * <pre>
 * // Using pre-defined constants (recommended)
 * Action action1 = StorageActions.GET_OBJECT;
 *
 * // Using wildcard
 * Action action2 = StorageActions.ALL; // storage:*
 *
 * // Creating custom action
 * Action action3 = Action.of("customService:CustomOperation");
 * </pre>
 */
public final class Action {
  private final String service;
  private final String operation;

  private Action(String service, String operation) {
    if (service == null || service.trim().isEmpty()) {
      throw new InvalidArgumentException("Action service cannot be null or empty");
    }
    if (operation == null || operation.trim().isEmpty()) {
      throw new InvalidArgumentException("Action operation cannot be null or empty");
    }
    this.service = service.trim();
    this.operation = operation.trim();
  }

  /**
   * Creates an Action from a string representation.
   *
   * @param action the action string in format "service:operation" (e.g., "storage:GetObject")
   * @return the Action object
   * @throws InvalidArgumentException if the action format is invalid
   */
  public static Action of(String action) {
    if (action == null || action.trim().isEmpty()) {
      throw new InvalidArgumentException("Action string cannot be null or empty");
    }

    String trimmed = action.trim();
    int colonIndex = trimmed.indexOf(':');

    if (colonIndex <= 0 || colonIndex == trimmed.length() - 1) {
      throw new InvalidArgumentException(
          "Invalid action format: '" + action + "'. Expected format: 'service:operation'");
    }

    // Check for multiple colons
    if (trimmed.indexOf(':', colonIndex + 1) != -1) {
      throw new InvalidArgumentException(
          "Invalid action format: '" + action + "'. Expected format: 'service:operation'");
    }

    String service = trimmed.substring(0, colonIndex);
    String operation = trimmed.substring(colonIndex + 1);

    return new Action(service, operation);
  }

  /**
   * Creates a wildcard action for the specified service.
   *
   * @param service the service name
   * @return an Action representing all operations for the service
   */
  static Action wildcard(String service) {
    return new Action(service, "*");
  }

  /**
   * Returns the service component of this action.
   *
   * @return the service name
   */
  public String getService() {
    return service;
  }

  /**
   * Returns the operation component of this action.
   *
   * @return the operation name
   */
  public String getOperation() {
    return operation;
  }

  /**
   * Checks if this is a wildcard action (operation = "*").
   *
   * @return true if this is a wildcard action
   */
  public boolean isWildcard() {
    return "*".equals(operation);
  }

  /**
   * Returns the string representation in format "service:operation".
   *
   * @return the action string
   */
  @JsonValue
  public String toActionString() {
    return service + ":" + operation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Action action = (Action) o;
    return service.equals(action.service) && operation.equals(action.operation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(service, operation);
  }

  @Override
  public String toString() {
    return toActionString();
  }
}
