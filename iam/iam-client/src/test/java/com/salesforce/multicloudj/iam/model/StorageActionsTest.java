package com.salesforce.multicloudj.iam.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class StorageActionsTest {

  @Test
  public void testGetObject() {
    assertEquals("storage:GetObject", StorageActions.GET_OBJECT.toActionString());
    assertEquals("storage", StorageActions.GET_OBJECT.getService());
    assertEquals("GetObject", StorageActions.GET_OBJECT.getOperation());
  }

  @Test
  public void testPutObject() {
    assertEquals("storage:PutObject", StorageActions.PUT_OBJECT.toActionString());
  }

  @Test
  public void testDeleteObject() {
    assertEquals("storage:DeleteObject", StorageActions.DELETE_OBJECT.toActionString());
  }

  @Test
  public void testListBucket() {
    assertEquals("storage:ListBucket", StorageActions.LIST_BUCKET.toActionString());
  }

  @Test
  public void testGetBucketLocation() {
    assertEquals("storage:GetBucketLocation", StorageActions.GET_BUCKET_LOCATION.toActionString());
  }

  @Test
  public void testCreateBucket() {
    assertEquals("storage:CreateBucket", StorageActions.CREATE_BUCKET.toActionString());
  }

  @Test
  public void testDeleteBucket() {
    assertEquals("storage:DeleteBucket", StorageActions.DELETE_BUCKET.toActionString());
  }

  @Test
  public void testWildcard() {
    assertEquals("storage:*", StorageActions.ALL.toActionString());
    assertTrue(StorageActions.ALL.isWildcard());
  }
}
