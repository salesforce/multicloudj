package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** Unit tests for PolicyDocument builder pattern. */
public class PolicyDocumentTest {

  private static final String TEST_VERSION = "TEST_VERSION";

  @Test
  public void testPolicyDocumentBuilder() {
    PolicyDocument policy =
        PolicyDocument.builder()
            .name("StorageAccess")
            .version("2024-01-01")
            .statement(
                Statement.builder()
                    .sid("StorageAccess")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .action(StorageActions.PUT_OBJECT)
                    .principal("arn:aws:iam::123456789012:user/ExampleUser")
                    .resource("storage://my-bucket/*")
                    .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
                    .build())
            .build();

    assertEquals("2024-01-01", policy.getVersion());
    assertEquals(1, policy.getStatements().size());

    Statement statement = policy.getStatements().get(0);
    assertEquals("StorageAccess", statement.getSid());
    assertEquals(Effect.ALLOW, statement.getEffect());
    assertEquals(2, statement.getActions().size());
    assertEquals(StorageActions.GET_OBJECT, statement.getActions().get(0));
    assertEquals(StorageActions.PUT_OBJECT, statement.getActions().get(1));
    assertEquals(
        Arrays.asList("arn:aws:iam::123456789012:user/ExampleUser"), statement.getPrincipals());
    assertEquals(Arrays.asList("storage://my-bucket/*"), statement.getResources());

    assertTrue(statement.getConditions().containsKey(ConditionOperator.STRING_EQUALS));
    assertEquals(
        "us-west-2",
        statement.getConditions().get(ConditionOperator.STRING_EQUALS).get("aws:RequestedRegion"));
  }

  @Test
  public void testMultipleStatements() {
    PolicyDocument policy =
        PolicyDocument.builder()
            .name("TestPolicy")
            .version(TEST_VERSION)
            .statement(
                Statement.builder()
                    .sid("ReadAccess")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .resource("storage://my-bucket/*")
                    .build())
            .statement(
                Statement.builder()
                    .sid("WriteAccess")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.PUT_OBJECT)
                    .resource("storage://my-bucket/*")
                    .build())
            .build();

    assertEquals(2, policy.getStatements().size());
    assertEquals("ReadAccess", policy.getStatements().get(0).getSid());
    assertEquals("WriteAccess", policy.getStatements().get(1).getSid());
  }

  @Test
  public void testEmptyPolicyThrowsException() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          PolicyDocument.builder().build();
        });
  }

  @Test
  public void testOptionalVersionBuildsSuccessfully() {
    PolicyDocument policy =
        PolicyDocument.builder()
            .name("TestPolicy")
            .statement(
                Statement.builder()
                    .sid("TestStatement")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .resource("storage://test-bucket/*")
                    .build())
            .build();
    assertNotNull(policy);
    assertNull(policy.getVersion());
  }

  @Test
  public void testStatementWithoutEffectThrowsException() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          PolicyDocument.builder()
              .name("TestPolicy")
              .version(TEST_VERSION)
              .statement(
                  Statement.builder()
                      .sid("TestStatement")
                      .action(StorageActions.GET_OBJECT)
                      .build())
              .build();
        });
  }

  @Test
  public void testStatementWithoutActionsThrowsException() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          PolicyDocument.builder()
              .name("TestPolicy")
              .version(TEST_VERSION)
              .statement(Statement.builder().sid("TestStatement").effect(Effect.ALLOW).build())
              .build();
        });
  }

  @Test
  public void testBuilderMethodsWithMultipleValues() {
    PolicyDocument policy =
        PolicyDocument.builder()
            .name("TestPolicy")
            .version(TEST_VERSION)
            .statement(
                Statement.builder()
                    .sid("TestStatement")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .action(StorageActions.PUT_OBJECT)
                    .action(StorageActions.DELETE_OBJECT)
                    .action(StorageActions.LIST_BUCKET)
                    .resource("storage://bucket1/*")
                    .resource("storage://bucket2/*")
                    .resource("storage://bucket3/*")
                    .resource("storage://bucket4/*")
                    .principal("principal1")
                    .principal("principal2")
                    .principal("principal3")
                    .principal("principal4")
                    .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
                    .condition(
                        ConditionOperator.DATE_GREATER_THAN,
                        "aws:CurrentTime",
                        "2024-01-01T00:00:00Z")
                    .build())
            .build();

    Statement statement = policy.getStatements().get(0);

    // Test actions
    assertEquals(4, statement.getActions().size());
    assertTrue(statement.getActions().contains(StorageActions.GET_OBJECT));
    assertTrue(statement.getActions().contains(StorageActions.PUT_OBJECT));
    assertTrue(statement.getActions().contains(StorageActions.DELETE_OBJECT));
    assertTrue(statement.getActions().contains(StorageActions.LIST_BUCKET));

    // Test resources
    assertEquals(4, statement.getResources().size());
    assertTrue(statement.getResources().contains("storage://bucket1/*"));
    assertTrue(statement.getResources().contains("storage://bucket2/*"));
    assertTrue(statement.getResources().contains("storage://bucket3/*"));
    assertTrue(statement.getResources().contains("storage://bucket4/*"));

    // Test principals
    assertEquals(4, statement.getPrincipals().size());
    assertTrue(statement.getPrincipals().contains("principal1"));
    assertTrue(statement.getPrincipals().contains("principal2"));
    assertTrue(statement.getPrincipals().contains("principal3"));
    assertTrue(statement.getPrincipals().contains("principal4"));

    // Test conditions
    assertTrue(statement.getConditions().containsKey(ConditionOperator.STRING_EQUALS));
    assertTrue(statement.getConditions().containsKey(ConditionOperator.DATE_GREATER_THAN));
  }

  @Test
  public void testAddNullStatement() {
    PolicyDocument policy =
        PolicyDocument.builder()
            .name("TestPolicy")
            .version(TEST_VERSION)
            .statement((Statement) null)
            .statement(
                Statement.builder()
                    .sid("ValidStatement")
                    .effect(Effect.ALLOW)
                    .action(StorageActions.GET_OBJECT)
                    .resource("storage://test-bucket/*")
                    .build())
            .build();

    assertEquals(1, policy.getStatements().size()); // Null statements are filtered out
    assertEquals("ValidStatement", policy.getStatements().get(0).getSid());
  }
}
