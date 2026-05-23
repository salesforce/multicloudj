package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class ComputeActionsTest {

  @Test
  public void testCreateInstance() {
    assertEquals("compute:CreateInstance", ComputeActions.CREATE_INSTANCE.toActionString());
  }

  @Test
  public void testDeleteInstance() {
    assertEquals("compute:DeleteInstance", ComputeActions.DELETE_INSTANCE.toActionString());
  }

  @Test
  public void testStartInstance() {
    assertEquals("compute:StartInstance", ComputeActions.START_INSTANCE.toActionString());
  }

  @Test
  public void testStopInstance() {
    assertEquals("compute:StopInstance", ComputeActions.STOP_INSTANCE.toActionString());
  }

  @Test
  public void testDescribeInstances() {
    assertEquals("compute:DescribeInstances", ComputeActions.DESCRIBE_INSTANCES.toActionString());
  }

  @Test
  public void testGetInstance() {
    assertEquals("compute:GetInstance", ComputeActions.GET_INSTANCE.toActionString());
  }

  @Test
  public void testWildcard() {
    assertEquals("compute:*", ComputeActions.ALL.toActionString());
    assertTrue(ComputeActions.ALL.isWildcard());
  }
}
