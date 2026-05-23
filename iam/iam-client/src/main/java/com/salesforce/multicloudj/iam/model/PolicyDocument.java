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
 * <p>This class provides a cloud-agnostic way to define IAM policies that can be translated to AWS,
 * GCP, or AliCloud native formats. The policy uses a builder pattern to prevent JSON parsing errors
 * and provides type safety.
 *
 * <p>Usage example:
 *
 * <pre>
 * PolicyDocument policy = PolicyDocument.builder()
 *     .statement(Statement.builder()
 *         .sid("StorageAccess")
 *         .effect(Effect.ALLOW)
 *         .action(StorageActions.GET_OBJECT)
 *         .action(StorageActions.PUT_OBJECT)
 *         .principal("arn:aws:iam::123456789012:user/ExampleUser")
 *         .resource("storage://my-bucket/*")
 *         .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
 *         .build())
 *     .build();
 * </pre>
 */
@Getter
public class PolicyDocument {
  private final String name;
  private final String version;
  private final List<Statement> statements;

  @Builder
  private PolicyDocument(String name, String version, @Singular List<Statement> statements) {
    // Filter out null statements and validate at least one exists
    List<Statement> filteredStatements =
        statements != null
            ? statements.stream()
                .filter(Objects::nonNull)
                .collect(java.util.stream.Collectors.toList())
            : new java.util.ArrayList<>();

    if (filteredStatements.isEmpty()) {
      throw new InvalidArgumentException("At least one statement is required");
    }

    this.name = name;
    this.version = version;
    this.statements = filteredStatements;
  }
}
