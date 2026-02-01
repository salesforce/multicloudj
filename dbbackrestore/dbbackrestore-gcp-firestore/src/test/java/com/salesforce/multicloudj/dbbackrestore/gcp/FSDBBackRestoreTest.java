package com.salesforce.multicloudj.dbbackrestore.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.firestore.admin.v1.Backup;
import com.google.firestore.admin.v1.ListBackupsResponse;
import com.google.firestore.admin.v1.RestoreDatabaseRequest;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackrestore.client.DBBackRestoreClient;
import com.salesforce.multicloudj.dbbackrestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackrestore.driver.RestoreRequest;
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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for FSDBBackRestore.
 */
@ExtendWith(MockitoExtension.class)
public class FSDBBackRestoreTest {

    @Mock
    private FirestoreAdminClient mockFirestoreClient;

    private FSDBBackRestore dbBackRestore;
    private static final String PROJECT_ID = "my-project";
    private static final String DATABASE_ID = "(default)";
    private static final String LOCATION = "us-central1";

    @BeforeEach
    void setUp() {
        dbBackRestore = new FSDBBackRestore.Builder()
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

        List<com.salesforce.multicloudj.dbbackrestore.driver.Backup> backups = dbBackRestore.listBackups();

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

        com.salesforce.multicloudj.dbbackrestore.driver.Backup result =
                dbBackRestore.getBackup("projects/my-project/locations/us-central1/backups/backup-123");

        assertNotNull(result);
        assertEquals("projects/my-project/locations/us-central1/backups/backup-123", result.getBackupId());
        assertEquals(BackupStatus.AVAILABLE, result.getStatus());
        assertEquals(2048L, result.getSizeInBytes());
        assertNull(result.getVaultId());
        verify(mockFirestoreClient, times(1)).getBackup("projects/my-project/locations/us-central1/backups/backup-123");
    }

    @Test
    void testGetBackupStatus() {
        Backup backup = Backup.newBuilder()
                .setName("projects/my-project/locations/us-central1/backups/backup-456")
                .setDatabase("projects/my-project/databases/(default)")
                .setState(Backup.State.CREATING)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(Instant.now().plusSeconds(86400).getEpochSecond()).build())
                .build();

        when(mockFirestoreClient.getBackup("projects/my-project/locations/us-central1/backups/backup-456"))
                .thenReturn(backup);

        BackupStatus status = dbBackRestore.getBackupStatus("projects/my-project/locations/us-central1/backups/backup-456");

        assertEquals(BackupStatus.CREATING, status);
        verify(mockFirestoreClient, times(1)).getBackup("projects/my-project/locations/us-central1/backups/backup-456");
    }

    @Test
    void testRestoreBackup() {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("projects/my-project/locations/us-central1/backups/backup-789")
                .targetResource("restored-db")
                .build();

        when(mockFirestoreClient.restoreDatabaseAsync(any(RestoreDatabaseRequest.class)))
                .thenReturn(null);

        dbBackRestore.restoreBackup(request);

        ArgumentCaptor<RestoreDatabaseRequest> captor = ArgumentCaptor.forClass(RestoreDatabaseRequest.class);
        verify(mockFirestoreClient, times(1)).restoreDatabaseAsync(captor.capture());

        RestoreDatabaseRequest capturedRequest = captor.getValue();
        assertEquals("projects/my-project/locations/us-central1/backups/backup-789", capturedRequest.getBackup());
        assertTrue(capturedRequest.getDatabaseId().contains("restored-db"));
    }

    @Test
    void testConvertBackupState() {
        // Test via getBackupStatus
        Backup readyBackup = Backup.newBuilder()
                .setName("backup-1")
                .setDatabase("db")
                .setState(Backup.State.READY)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(Instant.now().plusSeconds(86400).getEpochSecond()).build())
                .build();

        when(mockFirestoreClient.getBackup("backup-1"))
                .thenReturn(readyBackup);
        assertEquals(BackupStatus.AVAILABLE, dbBackRestore.getBackupStatus("backup-1"));

        Backup creatingBackup = Backup.newBuilder()
                .setName("backup-2")
                .setDatabase("db")
                .setState(Backup.State.CREATING)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(Instant.now().plusSeconds(86400).getEpochSecond()).build())
                .build();

        when(mockFirestoreClient.getBackup("backup-2"))
                .thenReturn(creatingBackup);
        assertEquals(BackupStatus.CREATING, dbBackRestore.getBackupStatus("backup-2"));

        Backup notAvailableBackup = Backup.newBuilder()
                .setName("backup-3")
                .setDatabase("db")
                .setState(Backup.State.NOT_AVAILABLE)
                .setSnapshotTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()).build())
                .setExpireTime(Timestamp.newBuilder().setSeconds(Instant.now().plusSeconds(86400).getEpochSecond()).build())
                .build();

        when(mockFirestoreClient.getBackup("backup-3"))
                .thenReturn(notAvailableBackup);
        assertEquals(BackupStatus.FAILED, dbBackRestore.getBackupStatus("backup-3"));
    }

    @Test
    void testGetException() {
        // Test non-ApiException cases - should map to UnknownException
        assertEquals(UnknownException.class, dbBackRestore.getException(new RuntimeException("Generic error")));

        // Test ApiException with NOT_FOUND -> ResourceNotFoundException
        ApiException apiException = org.mockito.Mockito.mock(ApiException.class);
        StatusCode mockStatusCode = org.mockito.Mockito.mock(StatusCode.class);
        when(apiException.getStatusCode()).thenReturn(mockStatusCode);
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
        assertEquals(ResourceNotFoundException.class, dbBackRestore.getException(apiException));

        // Test ApiException with ALREADY_EXISTS -> ResourceAlreadyExistsException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);
        assertEquals(ResourceAlreadyExistsException.class, dbBackRestore.getException(apiException));

        // Test ApiException with PERMISSION_DENIED -> UnAuthorizedException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.PERMISSION_DENIED);
        assertEquals(UnAuthorizedException.class, dbBackRestore.getException(apiException));

        // Test ApiException with UNAUTHENTICATED -> UnAuthorizedException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.UNAUTHENTICATED);
        assertEquals(UnAuthorizedException.class, dbBackRestore.getException(apiException));

        // Test ApiException with INTERNAL -> UnknownException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.INTERNAL);
        assertEquals(UnknownException.class, dbBackRestore.getException(apiException));

        // Test ApiException with INVALID_ARGUMENT -> UnknownException
        when(mockStatusCode.getCode()).thenReturn(StatusCode.Code.INVALID_ARGUMENT);
        assertEquals(UnknownException.class, dbBackRestore.getException(apiException));
    }

    @Test
    void testBuilder() {
        FSDBBackRestore.Builder builder = new FSDBBackRestore.Builder();
        assertNotNull(builder);

        // Test that building with all required fields creates an instance
        FSDBBackRestore instance = builder
                .withFirestoreAdminClient(mockFirestoreClient)
                .withRegion(LOCATION)
                .withResourceName(DATABASE_ID)
                .build();

        assertNotNull(instance);
        assertEquals("gcp-firestore", instance.getProviderId());
    }

    @Test
    void testDefaultConstructor() {
        FSDBBackRestore defaultInstance = new FSDBBackRestore();
        assertNotNull(defaultInstance);
        assertEquals("gcp-firestore", defaultInstance.getProviderId());

        FSDBBackRestore.Builder builder = defaultInstance.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testClose() {
        doNothing().when(mockFirestoreClient).close();
        dbBackRestore.close();
        verify(mockFirestoreClient, times(1)).close();
    }
}
