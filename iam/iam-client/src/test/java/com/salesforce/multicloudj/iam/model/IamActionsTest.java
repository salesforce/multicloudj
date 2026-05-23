package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class IamActionsTest {

  @Test
  public void testAssumeRole() {
    assertEquals("iam:AssumeRole", IamActions.ASSUME_ROLE.toActionString());
  }

  @Test
  public void testCreateRole() {
    assertEquals("iam:CreateRole", IamActions.CREATE_ROLE.toActionString());
  }

  @Test
  public void testDeleteRole() {
    assertEquals("iam:DeleteRole", IamActions.DELETE_ROLE.toActionString());
  }

  @Test
  public void testGetRole() {
    assertEquals("iam:GetRole", IamActions.GET_ROLE.toActionString());
  }

  @Test
  public void testAttachRolePolicy() {
    assertEquals("iam:AttachRolePolicy", IamActions.ATTACH_ROLE_POLICY.toActionString());
  }

  @Test
  public void testDetachRolePolicy() {
    assertEquals("iam:DetachRolePolicy", IamActions.DETACH_ROLE_POLICY.toActionString());
  }

  @Test
  public void testPutRolePolicy() {
    assertEquals("iam:PutRolePolicy", IamActions.PUT_ROLE_POLICY.toActionString());
  }

  @Test
  public void testGetRolePolicy() {
    assertEquals("iam:GetRolePolicy", IamActions.GET_ROLE_POLICY.toActionString());
  }

  @Test
  public void testWildcard() {
    assertEquals("iam:*", IamActions.ALL.toActionString());
    assertTrue(IamActions.ALL.isWildcard());
  }
}
