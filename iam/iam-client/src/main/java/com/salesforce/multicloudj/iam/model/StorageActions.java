package com.salesforce.multicloudj.iam.model;

/**
 * Pre-defined storage service actions for IAM policies.
 *
 * <p>These constants represent common storage operations that can be used in policy statements
 * across AWS S3, GCP Cloud Storage, and other cloud providers.
 */
public final class StorageActions {
  private StorageActions() {
    // Prevent instantiation
  }

  /** Action to read objects from storage */
  public static final Action GET_OBJECT = Action.of("storage:GetObject");

  /** Action to write/upload objects to storage */
  public static final Action PUT_OBJECT = Action.of("storage:PutObject");

  /** Action to delete objects from storage */
  public static final Action DELETE_OBJECT = Action.of("storage:DeleteObject");

  /** Action to list objects in a bucket/container */
  public static final Action LIST_BUCKET = Action.of("storage:ListBucket");

  /** Action to get bucket location/metadata */
  public static final Action GET_BUCKET_LOCATION = Action.of("storage:GetBucketLocation");

  /** Action to create a new bucket/container */
  public static final Action CREATE_BUCKET = Action.of("storage:CreateBucket");

  /** Action to delete a bucket/container */
  public static final Action DELETE_BUCKET = Action.of("storage:DeleteBucket");

  /** Wildcard action representing all storage operations */
  public static final Action ALL = Action.wildcard("storage");
}
