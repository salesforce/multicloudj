package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.List;
import java.util.Objects;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

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
 *     .version("2012-10-17")
 *     .statement(Statement.builder()
 *         .sid("StorageAccess")
 *         .effect("Allow")
 *         .action("storage:GetObject")
 *         .action("storage:PutObject")
 *         .principal("arn:aws:iam::123456789012:user/ExampleUser")
 *         .resource("storage://my-bucket/*")
 *         .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
 *         .build())
 *     .build();
 * </pre>
 */
@Getter
public class PolicyDocument {
  private final String version;
  private final List<Statement> statements;

  @Builder
  private PolicyDocument(String version, @Singular List<Statement> statements) {
    // Validate version is provided
    if (version == null) {
      throw new InvalidArgumentException("Version is required");
    }

    // Filter out null statements and validate at least one exists
    List<Statement> filteredStatements = statements != null
        ? statements.stream().filter(Objects::nonNull)
            .collect(java.util.stream.Collectors.toList())
        : new java.util.ArrayList<>();

    if (filteredStatements.isEmpty()) {
      throw new InvalidArgumentException("At least one statement is required");
    }

    this.version = version;
    this.statements = filteredStatements;
  }
}