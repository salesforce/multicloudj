package com.salesforce.multicloudj.dbbackuprestore.ali;

import com.aliyun.hbr20170908.Client;
import com.aliyun.hbr20170908.models.CreateRestoreJobRequest;
import com.aliyun.hbr20170908.models.CreateRestoreJobResponse;
import com.aliyun.hbr20170908.models.CreateRestoreJobResponseBody;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsRequest;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponse;
import com.aliyun.hbr20170908.models.DescribeOtsTableSnapshotsResponseBody;
import com.aliyun.hbr20170908.models.DescribeRestoreJobs2Request;
import com.aliyun.hbr20170908.models.DescribeRestoreJobs2Response;
import com.aliyun.hbr20170908.models.DescribeRestoreJobs2ResponseBody;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreStatus;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AliDBBackupRestore.
 */
@ExtendWith(MockitoExtension.class)
public class AliDBBackupRestoreTest {

    @Mock
    private Client mockHbrClient;

    private AliDBBackupRestore dbBackupRestore;
    private static final String REGION = "cn-shanghai";
    private static final String TABLE_NAME = "test-table";

    @BeforeEach
    void setUp() {
        dbBackupRestore = new AliDBBackupRestore.Builder()
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

        List<Backup> backups = dbBackupRestore.listBackups();

        assertEquals(2, backups.size());
        assertEquals("snapshot-1", backups.get(0).getBackupId());
        assertEquals(BackupStatus.AVAILABLE, backups.get(0).getStatus());
        assertEquals(2048L, backups.get(0).getSizeInBytes());
        assertEquals("vault-123", backups.get(0).getVaultId());
        assertEquals(BackupStatus.UNKNOWN, backups.get(1).getStatus());
        verify(mockHbrClient, times(1)).describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class));
    }

    @Test
    void testListBackupsEmptyResponse() throws Exception {
        DescribeOtsTableSnapshotsResponse response = new DescribeOtsTableSnapshotsResponse();
        response.setBody(null);

        when(mockHbrClient.describeOtsTableSnapshots(any(DescribeOtsTableSnapshotsRequest.class)))
                .thenReturn(response);

        List<Backup> backups = dbBackupRestore.listBackups();

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

        Backup backup = dbBackupRestore.getBackup("snapshot-123");

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
            dbBackupRestore.getBackup("non-existent-snapshot");
        });
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

        dbBackupRestore.restoreBackup(request);

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
            dbBackupRestore.restoreBackup(request);
        });
    }

    private Restore setupGetRestoreJobTest(String restoreJobId, String status, boolean includeCompletionTime)
            throws Exception {
        return setupGetRestoreJobTest(restoreJobId, status, includeCompletionTime, null);
    }

    private Restore setupGetRestoreJobTest(
            String restoreJobId, String status, boolean includeCompletionTime, String errorMessage) throws Exception {
        Instant creationTime = Instant.now().minusSeconds(300);
        Instant completionTime = Instant.now();

        DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobsRestoreJob restoreJob =
                new DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobsRestoreJob();
        restoreJob.setRestoreId(restoreJobId);
        restoreJob.setSnapshotId("snapshot-789");
        restoreJob.setTargetTableName("restored-table");
        restoreJob.setStatus(status);
        restoreJob.setCreatedTime(creationTime.getEpochSecond());

        if (includeCompletionTime) {
            restoreJob.setCompleteTime(completionTime.getEpochSecond());
        }
        if (errorMessage != null) {
            restoreJob.setErrorMessage(errorMessage);
        }

        DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobs restoreJobs =
                new DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobs();
        restoreJobs.setRestoreJob(Arrays.asList(restoreJob));

        DescribeRestoreJobs2ResponseBody responseBody = new DescribeRestoreJobs2ResponseBody();
        responseBody.setRestoreJobs(restoreJobs);

        DescribeRestoreJobs2Response response = new DescribeRestoreJobs2Response();
        response.setBody(responseBody);

        when(mockHbrClient.describeRestoreJobs2(any(DescribeRestoreJobs2Request.class)))
                .thenReturn(response);

        Restore restore = dbBackupRestore.getRestoreJob(restoreJobId);

        assertNotNull(restore);
        assertEquals(restoreJobId, restore.getRestoreId());
        assertEquals("snapshot-789", restore.getBackupId());
        assertEquals("restored-table", restore.getTargetResource());
        verify(mockHbrClient, times(1)).describeRestoreJobs2(any(DescribeRestoreJobs2Request.class));

        return restore;
    }

    @Test
    void testGetRestore_Job_Running() throws Exception {
        String restoreJobId = "restore-job-123";
        Restore restore = setupGetRestoreJobTest(restoreJobId, "RUNNING", false);
        assertEquals(RestoreStatus.RESTORING, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Completed() throws Exception {
        String restoreJobId = "restore-job-456";
        Restore restore = setupGetRestoreJobTest(restoreJobId, "COMPLETE", true);
        assertEquals(RestoreStatus.COMPLETED, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNotNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Failed() throws Exception {
        String restoreJobId = "restore-job-789";
        Restore restore = setupGetRestoreJobTest(restoreJobId, "FAILED", true);
        assertEquals(RestoreStatus.FAILED, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNotNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Failed_WithStatusMessage() throws Exception {
        String restoreJobId = "restore-job-789";
        String failureMessage = "Snapshot not found: snapshot-789";
        Restore restore = setupGetRestoreJobTest(restoreJobId, "FAILED", true, failureMessage);
        assertEquals(RestoreStatus.FAILED, restore.getStatus());
        assertEquals(failureMessage, restore.getStatusMessage());
        assertNotNull(restore.getStartTime());
        assertNotNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_NotFound() throws Exception {
        String restoreJobId = "non-existent-restore";

        DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobs restoreJobs =
                new DescribeRestoreJobs2ResponseBody.DescribeRestoreJobs2ResponseBodyRestoreJobs();
        restoreJobs.setRestoreJob(Arrays.asList());

        DescribeRestoreJobs2ResponseBody responseBody = new DescribeRestoreJobs2ResponseBody();
        responseBody.setRestoreJobs(restoreJobs);

        DescribeRestoreJobs2Response response = new DescribeRestoreJobs2Response();
        response.setBody(responseBody);

        when(mockHbrClient.describeRestoreJobs2(any(DescribeRestoreJobs2Request.class)))
                .thenReturn(response);

        assertThrows(ResourceNotFoundException.class, () -> {
            dbBackupRestore.getRestoreJob(restoreJobId);
        });
        verify(mockHbrClient, times(1)).describeRestoreJobs2(any(DescribeRestoreJobs2Request.class));
    }

    @Test
    void testGetException() {
        Class<? extends SubstrateSdkException> result = dbBackupRestore.getException(new RuntimeException("test"));
        assertNotNull(result);
    }

    @Test
    void testClose() throws Exception {
        // HBR client doesn't need explicit closing
        assertDoesNotThrow(() -> dbBackupRestore.close());
    }

    @Test
    void testDefaultConstructor() {
        AliDBBackupRestore dbrestore = new AliDBBackupRestore();
        Assertions.assertEquals("ali", dbrestore.getProviderId());

        AliDBBackupRestore.Builder builder = dbrestore.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testBuilderCreatesDefaultClient() throws Exception {
        // Test that build() creates a default HBR client when not provided
        // This exercises the hbrClient == null branch
        AliDBBackupRestore.Builder builder = new AliDBBackupRestore.Builder();

        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.build());
        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withRegion("region").build());

        // Provide accessKeyId and accessKeySecret so client can be created
        AliDBBackupRestore instance = builder
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
