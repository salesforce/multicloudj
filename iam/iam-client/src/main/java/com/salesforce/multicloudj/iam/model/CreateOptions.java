package com.salesforce.multicloudj.iam.model;

import java.util.Objects;
import lombok.Builder;
import lombok.Getter;

/**
 * Optional configuration for identity creation operations.
 *
 * <p>This class provides additional options that can be set during identity creation,
 * such as path specifications, session duration limits, and permission boundaries.
 *
 * <p>Permission boundary identifiers are provider-specific and translated internally
 * by the implementation layer. The client accepts the native format for the target
 * cloud provider.
 *
 * <p>Usage example:
 * <pre>
 * CreateOptions options = CreateOptions.builder()
 *     .path("/service-roles/")
 *     .maxSessionDuration(3600) // 1 hour in seconds
 *     .permissionBoundary("policy-identifier") // Provider-specific format
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
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateOptions that = (CreateOptions) o;
    return Objects.equals(path, that.path)
        && Objects.equals(maxSessionDuration, that.maxSessionDuration)
        && Objects.equals(permissionBoundary, that.permissionBoundary);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, maxSessionDuration, permissionBoundary);
  }

  @Override
  public String toString() {
    return "CreateOptions{"
        + "path='" + path + '\''
        + ", maxSessionDuration=" + maxSessionDuration
        + ", permissionBoundary='" + permissionBoundary + '\''
        + '}';
  }
}