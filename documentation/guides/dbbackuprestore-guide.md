---
layout: default
title: How to DB Backup & Restore
nav_order: 6
parent: Usage Guides
---
# DB Backup & Restore

The `DBBackupRestoreClient` class in the `multicloudj` library provides a cloud-agnostic interface for listing, inspecting, and restoring database backups across AWS, GCP, and Alibaba Cloud.

Restore operations are **asynchronous** — `restoreBackup` submits the job and returns an ID that you can poll via `getRestoreJob`.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|--------------|---------------|--------------|----------------|----------|
| **List Backups** | ✅ Supported | ✅ Supported | ✅ Supported | Returns all available backups for the resource |
| **Get Backup** | ✅ Supported | ✅ Supported | ✅ Supported | ALI uses a list-then-filter pattern |
| **Restore Backup** | ✅ Supported | ✅ Supported | ✅ Supported | Async — returns a restore job ID |
| **Get Restore Job** | ✅ Supported | ✅ Supported | ✅ Supported | GCP returns a long-running operation name |
| **KMS Encryption on Restore** | ✅ Supported | ✅ Supported | ❌ Not Supported | Customer-managed key for restored resource |
| **IAM Role for Restore** | ❌ Not Applicable | ✅ Supported | ❌ Not Applicable | AWS requires an IAM role ARN in RestoreRequest |
| **Vault-based Restore** | ❌ Not Applicable | ❌ Not Applicable | ✅ Supported | ALI requires a vault ID in RestoreRequest |

### Configuration Options

| Configuration | GCP Firestore | AWS DynamoDB | ALI Tablestore | Comments |
|---------------|---------------|--------------|----------------|----------|
| **Region Support** | ✅ Supported | ✅ Supported | ✅ Supported | Region where the database is located |
| **Resource Name** | ✅ Supported | ✅ Supported | ✅ Supported | Firestore database ID / DynamoDB table ARN / TableStore table name |

### Provider-Specific Notes

**AWS DynamoDB (`aws`)**
- `resourceName` is the full DynamoDB table ARN (e.g., `arn:aws:dynamodb:us-west-2:123456789012:table/my-table`).
- Restore operations require a `roleId` — an IAM Role ARN that AWS Backup assumes to perform the restore.
- Backup metadata maps to AWS Backup recovery points. Vault information is available in the `Backup.vaultId` field.
- `BackupStatus.DELETED` corresponds to expired recovery points.

**GCP Firestore (`gcp-firestore`)**
- `resourceName` is the Firestore database ID (e.g., `my-database` or `(default)`).
- Restore returns a long-running operation name. Poll `getRestoreJob(operationName)` to track completion.
- `targetResource` in `RestoreRequest` must be the target database ID.
- KMS encryption is supported via `kmsEncryptionKeyId` on restore.

**Alibaba Tablestore (`ali`)**
- `resourceName` is the TableStore table name.
- Restore operations require a `vaultId` — the HBR (Hybrid Backup Recovery) vault that stores the backup.
- `getBackup(id)` uses a list-then-filter pattern as no direct describe API exists.
- Restored tables are never overwritten — restore will fail if the target already exists.

---

## Creating a Client

```java
DBBackupRestoreClient client = DBBackupRestoreClient.builder("aws")
    .withRegion("us-west-2")
    .withResourceName("arn:aws:dynamodb:us-west-2:123456789012:table/my-table")
    .build();
```

For GCP Firestore:

```java
DBBackupRestoreClient client = DBBackupRestoreClient.builder("gcp-firestore")
    .withRegion("us-central1")
    .withResourceName("my-database")
    .build();
```

For Alibaba Tablestore:

```java
DBBackupRestoreClient client = DBBackupRestoreClient.builder("ali")
    .withRegion("cn-hangzhou")
    .withResourceName("my-table")
    .build();
```

---

## Listing Backups

Returns all available backups for the configured resource.

```java
List<Backup> backups = client.listBackups();
for (Backup backup : backups) {
    System.out.println("ID:      " + backup.getBackupId());
    System.out.println("Status:  " + backup.getStatus());
    System.out.println("Created: " + backup.getCreationTime());
    System.out.println("Expires: " + backup.getExpiryTime());   // null if no expiry
    System.out.println("Size:    " + backup.getSizeInBytes());  // -1 if unavailable
    System.out.println("Vault:   " + backup.getVaultId());
}
```

### Backup Status Values

| Status | Meaning |
|--------|---------|
| `CREATING` | Backup is being created |
| `AVAILABLE` | Backup is ready for restore |
| `DELETING` | Backup is being deleted |
| `DELETED` | Backup has been deleted or expired |
| `FAILED` | Backup creation failed |
| `UNKNOWN` | Status could not be determined |

---

## Getting a Specific Backup

```java
Backup backup = client.getBackup("backup-id-or-arn");
System.out.println("Status: " + backup.getStatus());
```

---

## Restoring a Backup

Restore operations are asynchronous. `restoreBackup` submits the job and returns a restore ID (or operation name on GCP) for tracking.

### AWS

```java
RestoreRequest request = RestoreRequest.builder()
    .backupId("arn:aws:backup:us-west-2:123456789012:recovery-point:abc123")
    .targetResource("my-restored-table")         // target DynamoDB table name
    .roleId("arn:aws:iam::123456789012:role/BackupRestoreRole")  // required
    .kmsEncryptionKeyId("arn:aws:kms:us-west-2:123456789012:key/key-id")  // optional
    .build();

String restoreId = client.restoreBackup(request);
```

### GCP Firestore

```java
RestoreRequest request = RestoreRequest.builder()
    .backupId("projects/my-project/locations/us-central1/backups/backup-id")
    .targetResource("my-restored-database")      // target Firestore database ID
    .kmsEncryptionKeyId("projects/my-project/locations/us-central1/keyRings/ring/cryptoKeys/key")  // optional
    .build();

String operationName = client.restoreBackup(request);  // returns long-running operation name
```

### Alibaba Tablestore

```java
RestoreRequest request = RestoreRequest.builder()
    .backupId("snapshot-id")
    .targetResource("my-restored-table")         // target table name
    .vaultId("vault-id")                         // required
    .build();

String restoreId = client.restoreBackup(request);
```

---

## Tracking a Restore Job

Poll `getRestoreJob` with the ID returned by `restoreBackup` to track progress.

```java
Restore restore = client.getRestoreJob(restoreId);
System.out.println("Status:  " + restore.getStatus());
System.out.println("Started: " + restore.getStartTime());
System.out.println("Ended:   " + restore.getEndTime());     // null if still in progress
System.out.println("Message: " + restore.getStatusMessage());
```

### Restore Status Values

| Status | Meaning |
|--------|---------|
| `RESTORING` | Restore operation is in progress |
| `COMPLETED` | Restore finished successfully |
| `FAILED` | Restore operation failed |
| `UNKNOWN` | Status could not be determined |

### Polling Until Completion

```java
Restore restore;
do {
    Thread.sleep(10_000);
    restore = client.getRestoreJob(restoreId);
} while (restore.getStatus() == RestoreStatus.RESTORING);

if (restore.getStatus() == RestoreStatus.COMPLETED) {
    System.out.println("Restore completed: " + restore.getTargetResource());
} else {
    System.out.println("Restore failed: " + restore.getStatusMessage());
}
```

---

## Error Handling

All operations may throw `SubstrateSdkException` subclasses:

| Exception | Cause |
|-----------|-------|
| `ResourceNotFoundException` | Backup or restore job does not exist |
| `ResourceAlreadyExistsException` | Target resource already exists (GCP) |
| `UnAuthorizedException` | Insufficient permissions to perform the operation |
| `SubstrateSdkException` | Catch-all for provider errors |

```java
try {
    Backup backup = client.getBackup("backup-id");
} catch (ResourceNotFoundException e) {
    System.out.println("Backup not found");
} catch (SubstrateSdkException e) {
    e.printStackTrace();
}
```

---

## Closing the Client

```java
client.close();
```

Use try-with-resources for guaranteed cleanup:

```java
try (DBBackupRestoreClient client = DBBackupRestoreClient.builder("aws")
        .withRegion("us-west-2")
        .withResourceName("arn:aws:dynamodb:us-west-2:123456789012:table/my-table")
        .build()) {
    List<Backup> backups = client.listBackups();
}
```
