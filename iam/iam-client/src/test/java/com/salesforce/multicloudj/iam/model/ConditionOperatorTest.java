package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class ConditionOperatorTest {

  @Test
  public void testStringOperators() {
    assertEquals("stringEquals", ConditionOperator.STRING_EQUALS.getValue());
    assertEquals("stringNotEquals", ConditionOperator.STRING_NOT_EQUALS.getValue());
    assertEquals("stringLike", ConditionOperator.STRING_LIKE.getValue());
    assertEquals("stringNotLike", ConditionOperator.STRING_NOT_LIKE.getValue());
  }

  @Test
  public void testNumericOperators() {
    assertEquals("numericEquals", ConditionOperator.NUMERIC_EQUALS.getValue());
    assertEquals("numericLessThan", ConditionOperator.NUMERIC_LESS_THAN.getValue());
    assertEquals("numericGreaterThan", ConditionOperator.NUMERIC_GREATER_THAN.getValue());
  }

  @Test
  public void testDateOperators() {
    assertEquals("dateEquals", ConditionOperator.DATE_EQUALS.getValue());
    assertEquals("dateLessThan", ConditionOperator.DATE_LESS_THAN.getValue());
    assertEquals("dateGreaterThan", ConditionOperator.DATE_GREATER_THAN.getValue());
  }

  @Test
  public void testOtherOperators() {
    assertEquals("bool", ConditionOperator.BOOL.getValue());
    assertEquals("ipAddress", ConditionOperator.IP_ADDRESS.getValue());
    assertEquals("notIpAddress", ConditionOperator.NOT_IP_ADDRESS.getValue());
  }

  @Test
  public void testToString() {
    assertEquals("stringEquals", ConditionOperator.STRING_EQUALS.toString());
  }
}
