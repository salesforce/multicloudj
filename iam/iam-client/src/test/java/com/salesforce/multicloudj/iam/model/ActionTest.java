package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

public class ActionTest {

  @Test
  public void testValidActionParsing() {
    Action action = Action.of("storage:GetObject");
    assertEquals("storage", action.getService());
    assertEquals("GetObject", action.getOperation());
    assertEquals("storage:GetObject", action.toActionString());
    assertFalse(action.isWildcard());
  }

  @Test
  public void testWildcardAction() {
    Action action = Action.of("storage:*");
    assertEquals("storage", action.getService());
    assertEquals("*", action.getOperation());
    assertEquals("storage:*", action.toActionString());
    assertTrue(action.isWildcard());
  }

  @Test
  public void testInvalidFormatNoColon() {
    assertThrows(InvalidArgumentException.class, () -> Action.of("invalid"));
  }

  @Test
  public void testInvalidFormatMultipleColons() {
    assertThrows(InvalidArgumentException.class, () -> Action.of("too:many:parts"));
  }

  @Test
  public void testInvalidFormatEmptyService() {
    assertThrows(InvalidArgumentException.class, () -> Action.of(":GetObject"));
  }

  @Test
  public void testInvalidFormatEmptyOperation() {
    assertThrows(InvalidArgumentException.class, () -> Action.of("storage:"));
  }

  @Test
  public void testNullAction() {
    assertThrows(InvalidArgumentException.class, () -> Action.of(null));
  }

  @Test
  public void testEmptyAction() {
    assertThrows(InvalidArgumentException.class, () -> Action.of(""));
  }

  @Test
  public void testWhitespaceAction() {
    assertThrows(InvalidArgumentException.class, () -> Action.of("   "));
  }

  @Test
  public void testActionWithWhitespace() {
    Action action = Action.of("  storage : GetObject  ");
    assertEquals("storage", action.getService());
    assertEquals("GetObject", action.getOperation());
  }

  @Test
  public void testActionEquality() {
    Action action1 = Action.of("storage:GetObject");
    Action action2 = Action.of("storage:GetObject");
    Action action3 = Action.of("storage:PutObject");

    assertEquals(action1, action2);
    assertEquals(action1.hashCode(), action2.hashCode());
    assertFalse(action1.equals(action3));
  }

  @Test
  public void testActionToString() {
    Action action = Action.of("compute:CreateInstance");
    assertEquals("compute:CreateInstance", action.toString());
  }
}
