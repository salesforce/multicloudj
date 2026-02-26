package com.salesforce.multicloudj.dbbackuprestore.gcp;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.firestore.admin.v1.Backup;
import com.google.firestore.admin.v1.Database;
import com.google.firestore.admin.v1.ListBackupsResponse;
import com.google.firestore.admin.v1.RestoreDatabaseMetadata;
import com.google.firestore.admin.v1.RestoreDatabaseRequest;
import com.google.longrunning.Operation;
import com.google.longrunning.OperationsClient;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import com.google.rpc.Status;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for FSDBBackupRestore.
 */
@ExtendWith(MockitoExtension.class)
public class FSDBBackupRestoreTest {

    @Mock
    private FirestoreAdminClient mockFirestoreClient;

    @Mock
    private OperationsClient mockOperationsClient;

    private FSDBBackupRestore dbBackupRestore;
    private static final String DATABASE_ID = "(default)";
    private static final String LOCATION = "us-central1";

    @BeforeEach
    void setUp() {
        dbBackupRestore = new FSDBBackupRestore.Builder()
                .withFirestoreAdminClient(mockFirestoreClient)
                .withRegion(LOCATION)
                .withResourceName(DATABASE_ID)
                .build();
    }

    @Test
    void testListBackups() {
        Backup backup1 = Backup.newBuilder()
                .setName("projects/my-project/locations/us-central1/backups/backup-1")
                .setDatabase("projects/my-project/databases/(default)")
                .setState(Backup.State.READY)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(Instant.now().plusSeconds(86400).getEpochSecond()).build())
                .build();

        Backup backup2 = Backup.newBuilder()
                .setName("projects/my-project/locations/us-central1/backups/backup-2")
                .setDatabase("projects/my-project/databases/(default)")
                .setState(Backup.State.CREATING)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(Instant.now().plusSeconds(86400).getEpochSecond()).build())
                .build();

        ListBackupsResponse response = ListBackupsResponse.newBuilder()
                .addAllBackups(Arrays.asList(backup1, backup2))
                .build();

        when(mockFirestoreClient.listBackups(LOCATION))
                .thenReturn(response);

        List<com.salesforce.multicloudj.dbbackuprestore.driver.Backup> backups = dbBackupRestore.listBackups();

        assertEquals(2, backups.size());
        assertEquals(BackupStatus.AVAILABLE, backups.get(0).getStatus());
        assertEquals(BackupStatus.CREATING, backups.get(1).getStatus());
        assertNull(backups.get(0).getVaultId()); // GCP doesn't use vaults
        verify(mockFirestoreClient, times(1)).listBackups(LOCATION);
    }

    @Test
    void testGetBackup() {
        Instant snapshotTime = Instant.now();
        Instant expireTime = snapshotTime.plusSeconds(86400);

        Backup backup = Backup.newBuilder()
                .setName("projects/my-project/locations/us-central1/backups/backup-123")
                .setDatabase("projects/my-project/databases/(default)")
                .setState(Backup.State.READY)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(snapshotTime.getEpochSecond()).setNanos(snapshotTime.getNano()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(expireTime.getEpochSecond()).setNanos(expireTime.getNano()).build())
                .setStats(Backup.Stats.newBuilder().setSizeBytes(2048).build())
                .build();

        when(mockFirestoreClient.getBackup("projects/my-project/locations/us-central1/backups/backup-123"))
                .thenReturn(backup);

        com.salesforce.multicloudj.dbbackuprestore.driver.Backup result =
                dbBackupRestore.getBackup("projects/my-project/locations/us-central1/backups/backup-123");

        assertNotNull(result);
        assertEquals("projects/my-project/locations/us-central1/backups/backup-123", result.getBackupId());
        assertEquals(BackupStatus.AVAILABLE, result.getStatus());
        assertEquals(2048L, result.getSizeInBytes());
        assertNull(result.getVaultId());
        verify(mockFirestoreClient, times(1)).getBackup("projects/my-project/locations/us-central1/backups/backup-123");
    }

    @Test
    void testRestoreBackup() throws Exception {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("projects/my-project/locations/us-central1/backups/backup-789")
                .targetResource("restored-db")
                .build();

        // Mock OperationFuture
        @SuppressWarnings("unchecked")
        OperationFuture<Database, RestoreDatabaseMetadata> mockOperationFuture =
                org.mockito.Mockito.mock(OperationFuture.class);
        when(mockOperationFuture.getName()).thenReturn("operations/restore-job-123");

        when(mockFirestoreClient.restoreDatabaseAsync(any(RestoreDatabaseRequest.class)))
                .thenReturn(mockOperationFuture);

        String restoreId = dbBackupRestore.restoreBackup(request);

        assertNotNull(restoreId);
        assertEquals("operations/restore-job-123", restoreId);

        ArgumentCaptor<RestoreDatabaseRequest> captor = ArgumentCaptor.forClass(RestoreDatabaseRequest.class);
        verify(mockFirestoreClient, times(1)).restoreDatabaseAsync(captor.capture());

        RestoreDatabaseRequest capturedRequest = captor.getValue();
        assertEquals("projects/my-project/locations/us-central1/backups/backup-789", capturedRequest.getBackup());
        assertEquals("restored-db", capturedRequest.getDatabaseId());
        assertEquals("projects/my-project", capturedRequest.getParent());
    }

    @Test
    void testRestoreBackup_WithCmekEncryption() throws Exception {
        String kmsKeyName = "projects/my-project/locations/us-central1/keyRings/my-ring/cryptoKeys/my-key";
        RestoreRequest request = RestoreRequest.builder()
                .backupId("projects/my-project/locations/us-central1/backups/backup-789")
                .targetResource("restored-db")
                .kmsEncryptionKeyId(kmsKeyName)
                .build();

        @SuppressWarnings("unchecked")
        OperationFuture<Database, RestoreDatabaseMetadata> mockOperationFuture =
                org.mockito.Mockito.mock(OperationFuture.class);
        when(mockOperationFuture.getName()).thenReturn("operations/restore-job-456");

        when(mockFirestoreClient.restoreDatabaseAsync(any(RestoreDatabaseRequest.class)))
                .thenReturn(mockOperationFuture);

        String restoreId = dbBackupRestore.restoreBackup(request);

        assertEquals("operations/restore-job-456", restoreId);

        ArgumentCaptor<RestoreDatabaseRequest> captor = ArgumentCaptor.forClass(RestoreDatabaseRequest.class);
        verify(mockFirestoreClient, times(1)).restoreDatabaseAsync(captor.capture());

        RestoreDatabaseRequest capturedRequest = captor.getValue();
        assertTrue(capturedRequest.hasEncryptionConfig());
        assertEquals(
                kmsKeyName,
                capturedRequest.getEncryptionConfig().getCustomerManagedEncryption().getKmsKeyName());
    }

    private Restore setupGetRestoreJobTest(String restoreId, boolean isDone, boolean hasError, boolean includeEndTime) {
        Instant startTime = Instant.now().minusSeconds(300);
        Instant endTime = Instant.now();

        RestoreDatabaseMetadata.Builder metadataBuilder = RestoreDatabaseMetadata.newBuilder()
                .setBackup("projects/my-project/locations/us-central1/backups/backup-789")
                .setDatabase("projects/my-project/databases/restored-db")
                .setStartTime(Timestamp.newBuilder()
                        .setSeconds(startTime.getEpochSecond())
                        .setNanos(startTime.getNano())
                        .build());

        if (includeEndTime) {
            metadataBuilder.setEndTime(Timestamp.newBuilder()
                    .setSeconds(endTime.getEpochSecond())
                    .setNanos(endTime.getNano())
                    .build());
        }

        Operation.Builder operationBuilder = Operation.newBuilder()
                .setName(restoreId)
                .setDone(isDone)
                .setMetadata(Any.pack(metadataBuilder.build()));

        if (hasError) {
            operationBuilder.setError(Status.newBuilder()
                    .setCode(13)
                    .setMessage("Restore operation failed")
                    .build());
        }

        when(mockFirestoreClient.getOperationsClient()).thenReturn(mockOperationsClient);
        when(mockOperationsClient.getOperation(restoreId)).thenReturn(operationBuilder.build());

        Restore restore = dbBackupRestore.getRestoreJob(restoreId);

        assertNotNull(restore);
        assertEquals(restoreId, restore.getRestoreId());
        assertEquals("projects/my-project/locations/us-central1/backups/backup-789", restore.getBackupId());
        assertEquals("projects/my-project/databases/restored-db", restore.getTargetResource());
        verify(mockFirestoreClient, times(1)).getOperationsClient();
        verify(mockOperationsClient, times(1)).getOperation(restoreId);

        return restore;
    }

    @Test
    void testGetRestore_Job_Restoring() {
        String restoreId = "projects/my-project/locations/us-central1/operations/operation-123";
        Restore restore = setupGetRestoreJobTest(restoreId, false, false, false);

        assertEquals(RestoreStatus.RESTORING, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Completed() {
        String restoreId = "projects/my-project/locations/us-central1/operations/operation-456";
        Restore restore = setupGetRestoreJobTest(restoreId, true, false, true);

        assertEquals(RestoreStatus.COMPLETED, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNotNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Failed() {
        String restoreId = "projects/my-project/locations/us-central1/operations/operation-789";
        Restore restore = setupGetRestoreJobTest(restoreId, true, true, false);

        assertEquals(RestoreStatus.FAILED, restore.getStatus());
        assertNotNull(restore.getStartTime());
    }

    @Test
    void testGetException() {
        // Test non-ApiException cases - should map to UnknownException
        assertEquals(UnknownException.class, dbBackupRestore.getException(new RuntimeException("Generic error")));

        // Test ApiException with NOT_FOUND -> ResourceNotFoundException
        ApiException apiException = org.mockito.Mockito.mock(ApiException.class);
        StatusCode mockStatusCode = org.mockito.Mockito.mock(StatusCode.class);
        when(apiException.getStatusCode()).thenReturn(mockStatusCode);
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
        assertEquals(ResourceNotFoundException.class, dbBackupRestore.getException(apiException));

        // Test ApiException with ALREADY_EXISTS -> ResourceAlreadyExistsException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);
        assertEquals(ResourceAlreadyExistsException.class, dbBackupRestore.getException(apiException));

        // Test ApiException with PERMISSION_DENIED -> UnAuthorizedException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.PERMISSION_DENIED);
        assertEquals(UnAuthorizedException.class, dbBackupRestore.getException(apiException));

        // Test ApiException with UNAUTHENTICATED -> UnAuthorizedException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.UNAUTHENTICATED);
        assertEquals(UnAuthorizedException.class, dbBackupRestore.getException(apiException));

        // Test ApiException with INTERNAL -> UnknownException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.INTERNAL);
        assertEquals(UnknownException.class, dbBackupRestore.getException(apiException));

        // Test ApiException with INVALID_ARGUMENT -> UnknownException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.INVALID_ARGUMENT);
        assertEquals(UnknownException.class, dbBackupRestore.getException(apiException));
    }

    @Test
    void testBuilder() {
        FSDBBackupRestore.Builder builder = new FSDBBackupRestore.Builder();
        assertNotNull(builder);

        // Test that building with all required fields creates an instance
        FSDBBackupRestore instance = builder
                .withFirestoreAdminClient(mockFirestoreClient)
                .withRegion(LOCATION)
                .withResourceName(DATABASE_ID)
                .build();

        assertNotNull(instance);
        assertEquals("gcp-firestore", instance.getProviderId());
    }

    @Test
    void testDefaultConstructor() {
        FSDBBackupRestore defaultInstance = new FSDBBackupRestore();
        assertNotNull(defaultInstance);
        assertEquals("gcp-firestore", defaultInstance.getProviderId());

        FSDBBackupRestore.Builder builder = defaultInstance.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testClose() {
        doNothing().when(mockFirestoreClient).close();
        dbBackupRestore.close();
        verify(mockFirestoreClient, times(1)).close();
    }
}
