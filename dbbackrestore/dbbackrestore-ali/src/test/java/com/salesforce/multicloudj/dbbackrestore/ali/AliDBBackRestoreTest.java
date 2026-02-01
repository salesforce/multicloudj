package com.salesforce.multicloudj.dbbackrestore.ali;

import com.aliyun.hbr20170908.Client;
import com.aliyun.hbr20170908.models.CreateRestoreJobRequest;
import com.aliyun.hbr20170908.models.CreateRestoreJobResponse;
import com.aliyun.hbr20170908.models.CreateRestoreJobResponseBody;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsRequest;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponse;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponseBody;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackrestore.driver.Backup;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AliDBBackRestore.
 */
@ExtendWith(MockitoExtension.class)
public class AliDBBackRestoreTest {

    @Mock
    private Client mockHbrClient;

    private AliDBBackRestore dbBackRestore;
    private static final String REGION = "cn-shanghai";
    private static final String TABLE_NAME = "test-table";

    @BeforeEach
    void setUp() {
        dbBackRestore = new AliDBBackRestore.Builder()
                .withHbrClient(mockHbrClient)
                .withRegion(REGION)
                .withResourceName(TABLE_NAME)
                .build();
    }

    @Test
    void testListBackups() throws Exception {
        DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots snapshot1 =
                new DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots();
        snapshot1.setSnapshotId("snapshot-1");
        snapshot1.setTableName(TABLE_NAME);
        snapshot1.setStatus("COMPLETE");
        snapshot1.setCreatedTime(Instant.now().getEpochSecond());
        snapshot1.setRetention(86400L);
        snapshot1.setActualBytes("2048");
        snapshot1.setVaultId("vault-123");

        DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots snapshot2 =
                new DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots();
        snapshot2.setSnapshotId("snapshot-2");
        snapshot2.setTableName(TABLE_NAME);
        snapshot2.setStatus("RUNNING");
        snapshot2.setCreatedTime(Instant.now().getEpochSecond());
        snapshot2.setVaultId("vault-123");

        DescribeOtsTableSnapshotsResponseBody responseBody = new DescribeOtsTableSnapshotsResponseBody();
        responseBody.setSnapshots(Arrays.asList(snapshot1, snapshot2));

        DescribeOtsTableSnapshotsResponse response = new DescribeOtsTableSnapshotsResponse();
        response.setBody(responseBody);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response);

        List<Backup> backups = dbBackRestore.listBackups();

        assertEquals(2, backups.size());
        assertEquals("snapshot-1", backups.get(0).getBackupId());
        assertEquals(BackupStatus.AVAILABLE, backups.get(0).getStatus());
        assertEquals(2048L, backups.get(0).getSizeInBytes());
        assertEquals("vault-123", backups.get(0).getVaultId());
        assertEquals(BackupStatus.CREATING, backups.get(1).getStatus());
        verify(mockHbrClient, times(1)).describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class));
    }

    @Test
    void testListBackupsEmptyResponse() throws Exception {
        DescribeOtsTableSnapshotsResponse response = new DescribeOtsTableSnapshotsResponse();
        response.setBody(null);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response);

        List<Backup> backups = dbBackRestore.listBackups();

        assertTrue(backups.isEmpty());
        verify(mockHbrClient, times(1)).describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class));
    }

    @Test
    void testGetBackup() throws Exception {
        DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots snapshot =
                new DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots();
        snapshot.setSnapshotId("snapshot-123");
        snapshot.setTableName(TABLE_NAME);
        snapshot.setStatus("COMPLETE");
        snapshot.setCreatedTime(Instant.now().getEpochSecond());
        snapshot.setRetention(86400L);
        snapshot.setActualBytes("4096");
        snapshot.setVaultId("vault-abc");

        DescribeOtsTableSnapshotsResponseBody responseBody = new DescribeOtsTableSnapshotsResponseBody();
        responseBody.setSnapshots(Arrays.asList(snapshot));

        DescribeOtsTableSnapshotsResponse response = new DescribeOtsTableSnapshotsResponse();
        response.setBody(responseBody);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response);

        Backup backup = dbBackRestore.getBackup("snapshot-123");

        assertNotNull(backup);
        assertEquals("snapshot-123", backup.getBackupId());
        assertEquals(TABLE_NAME, backup.getResourceName());
        assertEquals(BackupStatus.AVAILABLE, backup.getStatus());
        assertEquals(4096L, backup.getSizeInBytes());
        assertEquals("vault-abc", backup.getVaultId());
        verify(mockHbrClient, times(1)).describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class));
    }

    @Test
    void testGetBackupNotFound() throws Exception {
        DescribeOtsTableSnapshotsResponseBody responseBody = new DescribeOtsTableSnapshotsResponseBody();
        responseBody.setSnapshots(Arrays.asList());

        DescribeOtsTableSnapshotsResponse response = new DescribeOtsTableSnapshotsResponse();
        response.setBody(responseBody);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response);

        assertThrows(SubstrateSdkException.class, () -> {
            dbBackRestore.getBackup("non-existent-snapshot");
        });
    }

    @Test
    void testGetBackupStatus() throws Exception {
        DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots snapshot =
                new DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots();
        snapshot.setSnapshotId("snapshot-456");
        snapshot.setTableName(TABLE_NAME);
        snapshot.setStatus("RUNNING");
        snapshot.setCreatedTime(Instant.now().getEpochSecond());
        snapshot.setVaultId("vault-xyz");

        DescribeOtsTableSnapshotsResponseBody responseBody = new DescribeOtsTableSnapshotsResponseBody();
        responseBody.setSnapshots(Arrays.asList(snapshot));

        DescribeOtsTableSnapshotsResponse response = new DescribeOtsTableSnapshotsResponse();
        response.setBody(responseBody);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response);

        BackupStatus status = dbBackRestore.getBackupStatus("snapshot-456");

        assertEquals(BackupStatus.CREATING, status);
        verify(mockHbrClient, times(1)).describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class));
    }

    @Test
    void testRestoreBackup() throws Exception {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("snapshot-789")
                .targetResource("restored-table")
                .vaultId("vault-restore")
                .build();

        CreateRestoreJobResponseBody responseBody = new CreateRestoreJobResponseBody();
        responseBody.setSuccess(true);
        responseBody.setRestoreId("restore-job-123");

        CreateRestoreJobResponse response = new CreateRestoreJobResponse();
        response.setBody(responseBody);

        when(mockHbrClient.createRestoreJob(any(CreateRestoreJobRequest.class)))
                .thenReturn(response);

        dbBackRestore.restoreBackup(request);

        ArgumentCaptor<CreateRestoreJobRequest> captor = ArgumentCaptor.forClass(CreateRestoreJobRequest.class);
        verify(mockHbrClient, times(1)).createRestoreJob(captor.capture());

        CreateRestoreJobRequest capturedRequest = captor.getValue();
        assertEquals("OTS_TABLE", capturedRequest.getRestoreType());
        assertEquals("snapshot-789", capturedRequest.getSnapshotId());
        assertEquals("vault-restore", capturedRequest.getVaultId());
    }

    @Test
    void testRestoreBackupFailure() throws Exception {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("snapshot-999")
                .vaultId("vault-fail")
                .build();
        when(mockHbrClient.createRestoreJob(any(CreateRestoreJobRequest.class)))
                .thenThrow(new RuntimeException("Restore failed"));

        assertThrows(UnknownException.class, () -> {
            dbBackRestore.restoreBackup(request);
        });
    }

    @Test
    void testConvertSnapshotStatus() throws Exception {
        // Test COMPLETE status
        DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots completeSnapshot =
                new DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots();
        completeSnapshot.setSnapshotId("snap-1");
        completeSnapshot.setTableName(TABLE_NAME);
        completeSnapshot.setStatus("COMPLETE");
        completeSnapshot.setCreatedTime(Instant.now().getEpochSecond());

        DescribeOtsTableSnapshotsResponseBody body1 = new DescribeOtsTableSnapshotsResponseBody();
        body1.setSnapshots(Arrays.asList(completeSnapshot));
        DescribeOtsTableSnapshotsResponse response1 = new DescribeOtsTableSnapshotsResponse();
        response1.setBody(body1);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response1);
        assertEquals(BackupStatus.AVAILABLE, dbBackRestore.getBackupStatus("snap-1"));

        // Test FAILED status
        DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots failedSnapshot =
                new DescribeOtsTableSnapshotsResponseBody.DescribeOtsTableSnapshotsResponseBodySnapshots();
        failedSnapshot.setSnapshotId("snap-2");
        failedSnapshot.setTableName(TABLE_NAME);
        failedSnapshot.setStatus("FAILED");
        failedSnapshot.setCreatedTime(Instant.now().getEpochSecond());

        DescribeOtsTableSnapshotsResponseBody body2 = new DescribeOtsTableSnapshotsResponseBody();
        body2.setSnapshots(Arrays.asList(failedSnapshot));
        DescribeOtsTableSnapshotsResponse response2 = new DescribeOtsTableSnapshotsResponse();
        response2.setBody(body2);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response2);
        assertEquals(BackupStatus.FAILED, dbBackRestore.getBackupStatus("snap-2"));
    }

    @Test
    void testGetException() {
        Class<? extends SubstrateSdkException> result = dbBackRestore.getException(new RuntimeException("test"));
        assertNotNull(result);
    }

    @Test
    void testClose() throws Exception {
        // HBR client doesn't need explicit closing
        assertDoesNotThrow(() -> dbBackRestore.close());
    }

    @Test
    void testDefaultConstructor() {
        AliDBBackRestore dbrestore = new AliDBBackRestore();
        Assertions.assertEquals("ali", dbrestore.getProviderId());

        AliDBBackRestore.Builder builder = dbrestore.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testBuilderCreatesDefaultClient() throws Exception {
        // Test that build() creates a default HBR client when not provided
        // This exercises the hbrClient == null branch
        AliDBBackRestore.Builder builder = new AliDBBackRestore.Builder();

        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.build());
        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withRegion("region").build());

        // Provide accessKeyId and accessKeySecret so client can be created
        AliDBBackRestore instance = builder
                .withRegion(REGION)
                .withResourceName(TABLE_NAME)
                .build();

        assertNotNull(instance);
        assertEquals("ali", instance.getProviderId());
        assertEquals(REGION, instance.getRegion());
        assertEquals(TABLE_NAME, instance.getResourceName());

        // Clean up
        instance.close();
    }
}
