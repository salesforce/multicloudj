package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Unit tests for Statement builder pattern and functionality. */
public class StatementTest {

  @Test
  public void testStatementBuilder() {
    Statement statement =
        Statement.builder()
            .sid("TestStatement")
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(StorageActions.PUT_OBJECT)
            .resource("storage://my-bucket/*")
            .principal("arn:aws:iam::123456789012:user/TestUser")
            .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
            .build();

    assertEquals("TestStatement", statement.getSid());
    assertEquals(Effect.ALLOW, statement.getEffect());
    assertEquals(2, statement.getActions().size());
    assertEquals(StorageActions.GET_OBJECT, statement.getActions().get(0));
    assertEquals(StorageActions.PUT_OBJECT, statement.getActions().get(1));
    assertEquals(Arrays.asList("storage://my-bucket/*"), statement.getResources());
    assertEquals(
        Arrays.asList("arn:aws:iam::123456789012:user/TestUser"), statement.getPrincipals());

    assertTrue(statement.getConditions().containsKey(ConditionOperator.STRING_EQUALS));
    assertEquals(
        "us-west-2",
        statement.getConditions().get(ConditionOperator.STRING_EQUALS).get("aws:RequestedRegion"));
  }

  @Test
  public void testStatementBuilderMinimal() {
    Statement statement =
        Statement.builder()
            .sid("MinimalStatement")
            .effect(Effect.DENY)
            .action(StorageActions.DELETE_OBJECT)
            .resource("storage://sensitive-bucket/*")
            .build();

    assertEquals("MinimalStatement", statement.getSid());
    assertEquals(Effect.DENY, statement.getEffect());
    assertEquals(1, statement.getActions().size());
    assertEquals(StorageActions.DELETE_OBJECT, statement.getActions().get(0));
    assertEquals(Arrays.asList("storage://sensitive-bucket/*"), statement.getResources());
    assertTrue(statement.getPrincipals().isEmpty());
    assertTrue(statement.getConditions().isEmpty());
  }

  @Test
  public void testStatementBuilderMultipleResources() {
    List<String> expectedResources = Arrays.asList("storage://bucket1/*", "storage://bucket2/*");

    Statement statement =
        Statement.builder()
            .sid("MultiResourceStatement")
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .resource("storage://bucket1/*")
            .resource("storage://bucket2/*")
            .build();

    assertEquals(expectedResources, statement.getResources());
  }

  @Test
  public void testStatementBuilderMultiplePrincipals() {
    List<String> expectedPrincipals =
        Arrays.asList(
            "arn:aws:iam::123456789012:user/User1", "arn:aws:iam::123456789012:user/User2");

    Statement statement =
        Statement.builder()
            .sid("MultiPrincipalStatement")
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .resource("storage://shared-bucket/*")
            .principal("arn:aws:iam::123456789012:user/User1")
            .principal("arn:aws:iam::123456789012:user/User2")
            .build();

    assertEquals(expectedPrincipals, statement.getPrincipals());
  }

  @Test
  public void testStatementBuilderMultipleConditions() {
    Statement statement =
        Statement.builder()
            .sid("MultiConditionStatement")
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .resource("storage://conditional-bucket/*")
            .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
            .condition(
                ConditionOperator.DATE_GREATER_THAN, "aws:CurrentTime", "2024-01-01T00:00:00Z")
            .build();

    assertTrue(statement.getConditions().containsKey(ConditionOperator.STRING_EQUALS));
    assertTrue(statement.getConditions().containsKey(ConditionOperator.DATE_GREATER_THAN));
    assertEquals(
        "us-west-2",
        statement.getConditions().get(ConditionOperator.STRING_EQUALS).get("aws:RequestedRegion"));
    assertEquals(
        "2024-01-01T00:00:00Z",
        statement.getConditions().get(ConditionOperator.DATE_GREATER_THAN).get("aws:CurrentTime"));
  }

  @Test
  public void testStatementWithoutSid() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .resource("storage://no-sid-bucket/*")
            .build();

    assertNull(statement.getSid());
    assertEquals(Effect.ALLOW, statement.getEffect());
  }

  @Test
  public void testEmptyStatementThrowsException() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          Statement.builder().build();
        });
  }

  @Test
  public void testStatementWithoutEffectThrowsException() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          Statement.builder()
              .sid("NoEffectStatement")
              .action(StorageActions.GET_OBJECT)
              .resource("storage://test-bucket/*")
              .build();
        });
  }

  @Test
  public void testStatementWithoutActionsThrowsException() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          Statement.builder()
              .sid("NoActionsStatement")
              .effect(Effect.ALLOW)
              .resource("storage://test-bucket/*")
              .build();
        });
  }

  @Test
  public void testStatementWithNullAction() {
    assertThrows(
        InvalidArgumentException.class,
        () -> {
          Statement.builder()
              .sid("NullActionStatement")
              .effect(Effect.ALLOW)
              .action((Action) null)
              .build();
        });
  }

  @Test
  public void testNullAndEmptyValueHandling() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .resource(null)
            .resource("")
            .resource("   ")
            .resource("storage://test-bucket/*")
            .principal(null)
            .principal("")
            .principal("   ")
            .principal("valid-principal")
            .condition(null, "key", "value")
            .condition(ConditionOperator.STRING_EQUALS, null, "value")
            .condition(ConditionOperator.STRING_EQUALS, "key", null)
            .condition(ConditionOperator.STRING_EQUALS, "aws:RequestedRegion", "us-west-2")
            .build();

    assertEquals(1, statement.getActions().size());
    assertEquals(StorageActions.GET_OBJECT, statement.getActions().get(0));

    assertEquals(1, statement.getResources().size());
    assertEquals("storage://test-bucket/*", statement.getResources().get(0));

    assertEquals(1, statement.getPrincipals().size());
    assertEquals("valid-principal", statement.getPrincipals().get(0));

    assertEquals(1, statement.getConditions().size());
    assertTrue(statement.getConditions().containsKey(ConditionOperator.STRING_EQUALS));
    assertEquals(
        "us-west-2",
        statement.getConditions().get(ConditionOperator.STRING_EQUALS).get("aws:RequestedRegion"));
  }

  @Test
  public void testMixedServiceActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(ComputeActions.CREATE_INSTANCE)
            .action(IamActions.ASSUME_ROLE)
            .build();

    assertEquals(3, statement.getActions().size());
    assertEquals(StorageActions.GET_OBJECT, statement.getActions().get(0));
    assertEquals(ComputeActions.CREATE_INSTANCE, statement.getActions().get(1));
    assertEquals(IamActions.ASSUME_ROLE, statement.getActions().get(2));
  }

  @Test
  public void testWildcardActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.ALL)
            .action(ComputeActions.ALL)
            .action(IamActions.ALL)
            .build();

    assertEquals(3, statement.getActions().size());
    assertTrue(statement.getActions().get(0).isWildcard());
    assertTrue(statement.getActions().get(1).isWildcard());
    assertTrue(statement.getActions().get(2).isWildcard());
  }

  @Test
  public void testCustomAction() {
    Action customAction = Action.of("customService:CustomOperation");
    Statement statement = Statement.builder().effect(Effect.ALLOW).action(customAction).build();

    assertEquals(1, statement.getActions().size());
    assertEquals("customService:CustomOperation", statement.getActions().get(0).toActionString());
  }

  @Test
  public void testGetActionsAsStrings() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(StorageActions.PUT_OBJECT)
            .build();

    List<String> actionStrings = statement.getActionsAsStrings();
    assertEquals(2, actionStrings.size());
    assertEquals("storage:GetObject", actionStrings.get(0));
    assertEquals("storage:PutObject", actionStrings.get(1));
  }

  @Test
  public void testGetConditionsAsStrings() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .condition(ConditionOperator.STRING_EQUALS, "key1", "value1")
            .condition(ConditionOperator.NUMERIC_LESS_THAN, "key2", 100)
            .build();

    var conditionStrings = statement.getConditionsAsStrings();
    assertEquals(2, conditionStrings.size());
    assertTrue(conditionStrings.containsKey("stringEquals"));
    assertTrue(conditionStrings.containsKey("numericLessThan"));
    assertEquals("value1", conditionStrings.get("stringEquals").get("key1"));
    assertEquals(100, conditionStrings.get("numericLessThan").get("key2"));
  }
}
