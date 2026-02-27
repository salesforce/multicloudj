---
layout: default
title: How to DB Backup Restore
nav_order: 6
parent: Usage Guides
---
# DBBackupRestoreClient

The `DBBackupRestoreClient` class in the `multicloudj` library provides a cloud-agnostic interface to interact with database backup and restore services like AWS Backup (DynamoDB), GCP Firestore Admin, and Alibaba Cloud HBR (TableStore).

This client enables listing backups, retrieving backup details, restoring backups to target resources, and tracking restore job status across multiple cloud providers with a consistent API.

---

## Feature Support Across Providers

### Core API Features

| Feature Name | GCP (Firestore) | AWS (DynamoDB) | ALI (TableStore) | Comments |
|--------------|------------------|----------------|-------------------|----------|
| **List Backups** | ✅ Supported | ✅ Supported | ✅ Supported | List all available backups for a resource |
| **Get Backup** | ✅ Supported | ✅ Supported | ✅ Supported | Retrieve details of a specific backup |
| **Restore Backup** | ✅ Supported | ✅ Supported | ✅ Supported | Initiate an async restore operation |
| **Get Restore Job** | ✅ Supported | ✅ Supported | ✅ Supported | Track the status of a restore operation |

### Configuration Options

| Configuration | GCP (Firestore) | AWS (DynamoDB) | ALI (TableStore) | Comments |
|---------------|------------------|----------------|-------------------|----------|
| **Region** | ✅ Required | ✅ Required | ✅ Required | Cloud region for the service |
| **Resource Name** | ✅ Required | ✅ Required | ✅ Required | Table name or ARN |

---

### Provider-Specific Notes

**AWS (DynamoDB via AWS Backup)**
- `resourceName` must be the full DynamoDB table ARN (e.g., `arn:aws:dynamodb:us-east-1:123456789012:table/my-table`).
- `restoreBackup` requires `roleId` (IAM role ARN for the restore job) and `targetResource` (target table name).
- `backupId` values are recovery point ARNs.
- `kmsEncryptionKeyId` can be specified to encrypt the restored table with a specific KMS key.

**GCP (Firestore)**
- `resourceName` is the Firestore collection/database name.
- `restoreBackup` requires `targetResource` (target database ID). The restore operation is asynchronous (long-running operation).
- `backupId` is the full Firestore backup resource name.

**Alibaba Cloud (TableStore via HBR)**
- `resourceName` is the TableStore table name.
- `restoreBackup` requires `vaultId` (HBR vault identifier).
- Backups are represented as OTS table snapshots.

---

## Creating the Client

### Basic Client

For AWS:
```java
DBBackupRestoreClient client = DBBackupRestoreClient.builder("aws")
    .withRegion("us-east-1")
    .withResourceName("arn:aws:dynamodb:us-east-1:123456789012:table/my-table")
    .build();
```

For GCP Firestore:
```java
DBBackupRestoreClient client = DBBackupRestoreClient.builder("gcp-firestore")
    .withRegion("projects/myproject/locations/nam5")
    .withResourceName("projects/myproject/databases/(default)/documents/docstore-test-1")
    .build();
```

Use the appropriate provider ID: `"aws"`, `"gcp-firestore"`, or `"ali"`. The client implements `AutoCloseable`; use try-with-resources or call `close()` when done.

---

## Listing Backups

```java
List<Backup> backups = client.listBackups();
for (Backup backup : backups) {
    System.out.println("Backup ID: " + backup.getBackupId());
    System.out.println("Status: " + backup.getStatus());
    System.out.println("Created: " + backup.getCreationTime());
    System.out.println("Size: " + backup.getSizeInBytes() + " bytes");
}
```

---

## Getting Backup Details

```java
Backup backup = client.getBackup("backup-id");
System.out.println("Resource: " + backup.getResourceName());
System.out.println("Status: " + backup.getStatus());
System.out.println("Description: " + backup.getDescription());
System.out.println("Expiry: " + backup.getExpiryTime());
```

### Backup Status Values

| Status | Description |
|--------|-------------|
| `CREATING` | Backup is being created |
| `AVAILABLE` | Backup is ready for restoration |
| `DELETING` | Backup is being deleted |
| `DELETED` | Backup has been deleted |
| `FAILED` | Backup operation failed |
| `UNKNOWN` | Status could not be determined |

---

## Restoring a Backup

Restore operations are asynchronous. The `restoreBackup` method returns a restore ID that can be used to track progress.

### AWS Example

```java
RestoreRequest request = RestoreRequest.builder()
    .backupId("arn:aws:backup:us-east-1:123456789012:recovery-point/abc-123")
    .targetResource("restored-table")
    .roleId("arn:aws:iam::123456789012:role/backup-restore-role")
    .kmsEncryptionKeyId("arn:aws:kms:us-east-1:123456789012:key/my-key-id")
    .build();

String restoreId = client.restoreBackup(request);
```

### GCP Firestore Example

```java
RestoreRequest request = RestoreRequest.builder()
    .backupId("projects/my-project/locations/us-east1/backups/my-backup")
    .targetResource("restored-database")
    .build();

String restoreId = client.restoreBackup(request);
```

### Alibaba Cloud Example

```java
RestoreRequest request = RestoreRequest.builder()
    .backupId("snapshot-id")
    .vaultId("vault-id")
    .build();

String restoreId = client.restoreBackup(request);
```

---

## Tracking Restore Status

```java
Restore restore = client.getRestoreJob(restoreId);
System.out.println("Restore ID: " + restore.getRestoreId());
System.out.println("Status: " + restore.getStatus());
System.out.println("Target: " + restore.getTargetResource());
System.out.println("Started: " + restore.getStartTime());
System.out.println("Message: " + restore.getStatusMessage());
```

### Restore Status Values

| Status | Description |
|--------|-------------|
| `RESTORING` | Restore operation is in progress |
| `COMPLETED` | Restore completed successfully |
| `FAILED` | Restore operation failed |
| `UNKNOWN` | Status could not be determined |

---

## Error Handling

### Exception Handling

All operations may throw `SubstrateSdkException`:

```java
try {
    List<Backup> backups = client.listBackups();
} catch (SubstrateSdkException e) {
    // Handle resource not found, access denied, etc.
    e.printStackTrace();
}
```
