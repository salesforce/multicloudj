package com.salesforce.multicloudj.iam.model;

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
}