package com.salesforce.multicloudj.dbbackuprestore.aws;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
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
import software.amazon.awssdk.services.backup.BackupClient;
import software.amazon.awssdk.services.backup.model.CalculatedLifecycle;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse;
import software.amazon.awssdk.services.backup.model.DescribeRestoreJobRequest;
import software.amazon.awssdk.services.backup.model.DescribeRestoreJobResponse;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceResponse;
import software.amazon.awssdk.services.backup.model.RecoveryPointByResource;
import software.amazon.awssdk.services.backup.model.RecoveryPointStatus;
import software.amazon.awssdk.services.backup.model.RestoreJobStatus;
import software.amazon.awssdk.services.backup.model.StartRestoreJobRequest;
import software.amazon.awssdk.services.backup.model.StartRestoreJobResponse;

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
 * Unit tests for AwsDBBackupRestore.
 */
@ExtendWith(MockitoExtension.class)
public class AwsDBBackupRestoreTest {

    @Mock
    private BackupClient mockBackupClient;

    private AwsDBBackupRestore dbBackupRestore;
    private static final String TABLE_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/test-table";
    private static final String IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/RestoreRole";

    @BeforeEach
    void setUp() {
        dbBackupRestore = new AwsDBBackupRestore.Builder()
                .withBackupClient(mockBackupClient)
                .withRegion("us-west-2")
                .withResourceName(TABLE_ARN)
                .build();
    }

    @Test
    void testListBackups() {
        RecoveryPointByResource recovery1 = RecoveryPointByResource.builder()
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:1")
                .backupVaultName("Default")
                .status(RecoveryPointStatus.COMPLETED)
                .creationDate(Instant.now())
                .backupSizeBytes(1024L)
                .build();

        RecoveryPointByResource recovery2 = RecoveryPointByResource.builder()
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:2")
                .backupVaultName("Default")
                .status(RecoveryPointStatus.CREATING)
                .creationDate(Instant.now())
                .backupSizeBytes(2048L)
                .build();

        ListRecoveryPointsByResourceResponse response = ListRecoveryPointsByResourceResponse.builder()
                .recoveryPoints(Arrays.asList(recovery1, recovery2))
                .build();

        when(mockBackupClient.listRecoveryPointsByResource(any(ListRecoveryPointsByResourceRequest.class)))
                .thenReturn(response);

        List<Backup> backups = dbBackupRestore.listBackups();

        assertEquals(2, backups.size());
        assertEquals(BackupStatus.AVAILABLE, backups.get(0).getStatus());
        assertEquals(BackupStatus.CREATING, backups.get(1).getStatus());
        assertEquals("Default", backups.get(0).getVaultId());
        verify(mockBackupClient, times(1)).listRecoveryPointsByResource(any(ListRecoveryPointsByResourceRequest.class));
    }

    @Test
    void testGetBackup() {
        Instant creationTime = Instant.now();
        Instant expiryTime = creationTime.plusSeconds(86400);

        // Mock listRecoveryPointsByResource to find the vault
        RecoveryPointByResource recovery = RecoveryPointByResource.builder()
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:123")
                .backupVaultName("MyVault")
                .backupSizeBytes(1024L)
                .build();

        ListRecoveryPointsByResourceResponse listResponse = ListRecoveryPointsByResourceResponse.builder()
                .recoveryPoints(Arrays.asList(recovery))
                .build();

        when(mockBackupClient.listRecoveryPointsByResource(any(ListRecoveryPointsByResourceRequest.class)))
                .thenReturn(listResponse);

        DescribeRecoveryPointResponse response = DescribeRecoveryPointResponse.builder()
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:123")
                .backupVaultName("MyVault")
                .status(RecoveryPointStatus.COMPLETED)
                .creationDate(creationTime)
                .calculatedLifecycle(CalculatedLifecycle.builder()
                        .deleteAt(expiryTime)
                        .build())
                .backupSizeInBytes(2048L)
                .build();

        when(mockBackupClient.describeRecoveryPoint(any(DescribeRecoveryPointRequest.class)))
                .thenReturn(response);

        Backup backup = dbBackupRestore.getBackup("arn:aws:backup:us-west-2:123456789012:recovery-point:123");

        assertNotNull(backup);
        assertEquals("arn:aws:backup:us-west-2:123456789012:recovery-point:123", backup.getBackupId());
        assertEquals(BackupStatus.AVAILABLE, backup.getStatus());
        assertEquals(creationTime, backup.getCreationTime());
        assertEquals(expiryTime, backup.getExpiryTime());
        assertEquals(2048L, backup.getSizeInBytes());
        assertEquals("MyVault", backup.getVaultId());
        verify(mockBackupClient, times(1)).describeRecoveryPoint(any(DescribeRecoveryPointRequest.class));
    }

    @Test
    void testRestoreBackup() {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("arn:aws:backup:us-west-2:123456789012:recovery-point:789")
                .targetResource("restored-table")
                .roleId(IAM_ROLE_ARN)
                .build();

        StartRestoreJobResponse response = StartRestoreJobResponse.builder()
                .restoreJobId("restore-job-123")
                .build();

        when(mockBackupClient.startRestoreJob(any(StartRestoreJobRequest.class)))
                .thenReturn(response);

        dbBackupRestore.restoreBackup(request);

        ArgumentCaptor<StartRestoreJobRequest> captor = ArgumentCaptor.forClass(StartRestoreJobRequest.class);
        verify(mockBackupClient, times(1)).startRestoreJob(captor.capture());

        StartRestoreJobRequest capturedRequest = captor.getValue();
        assertEquals("arn:aws:backup:us-west-2:123456789012:recovery-point:789", capturedRequest.recoveryPointArn());
        assertEquals(IAM_ROLE_ARN, capturedRequest.iamRoleArn());
        assertEquals("DynamoDB", capturedRequest.resourceType());
        assertTrue(capturedRequest.metadata().containsKey("targetTableName"));
        assertEquals("restored-table", capturedRequest.metadata().get("targetTableName"));
    }

    @Test
    void testRestoreBackupMissingRole() {
        RestoreRequest request = RestoreRequest.builder()
                .backupId("arn:aws:backup:us-west-2:123456789012:recovery-point:789")
                .targetResource("restored-table")
                .build();

        StartRestoreJobResponse response = StartRestoreJobResponse.builder()
                .restoreJobId("restore-job-123")
                .build();
        Assertions.assertThrows(IllegalArgumentException.class, () -> dbBackupRestore.restoreBackup(request));
    }

    private Restore setupGetRestoreJobTest(String restoreJobId, RestoreJobStatus status, boolean includeCompletionDate) {
        Instant creationTime = Instant.now().minusSeconds(300);
        Instant completionTime = Instant.now();

        DescribeRestoreJobResponse.Builder responseBuilder = DescribeRestoreJobResponse.builder()
                .restoreJobId(restoreJobId)
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:789")
                .createdResourceArn("arn:aws:dynamodb:us-west-2:123456789012:table/restored-table")
                .status(status)
                .creationDate(creationTime);

        if (includeCompletionDate) {
            responseBuilder.completionDate(completionTime);
        }

        when(mockBackupClient.describeRestoreJob(any(DescribeRestoreJobRequest.class)))
                .thenReturn(responseBuilder.build());

        Restore restore = dbBackupRestore.getRestoreJob(restoreJobId);

        assertNotNull(restore);
        assertEquals(restoreJobId, restore.getRestoreId());
        assertEquals("arn:aws:backup:us-west-2:123456789012:recovery-point:789", restore.getBackupId());
        assertEquals("arn:aws:dynamodb:us-west-2:123456789012:table/restored-table", restore.getTargetResource());
        verify(mockBackupClient, times(1)).describeRestoreJob(any(DescribeRestoreJobRequest.class));

        return restore;
    }

    @Test
    void testGetRestore_Job_Running() {
        String restoreJobId = "restore-job-123";
        Restore restore = setupGetRestoreJobTest(restoreJobId, RestoreJobStatus.RUNNING, false);

        assertEquals(RestoreStatus.RESTORING, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Completed() {
        String restoreJobId = "restore-job-456";
        Restore restore = setupGetRestoreJobTest(restoreJobId, RestoreJobStatus.COMPLETED, true);

        assertEquals(RestoreStatus.COMPLETED, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNotNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Failed() {
        String restoreJobId = "restore-job-789";
        Restore restore = setupGetRestoreJobTest(restoreJobId, RestoreJobStatus.FAILED, true);

        assertEquals(RestoreStatus.FAILED, restore.getStatus());
        assertNotNull(restore.getStartTime());
        assertNotNull(restore.getEndTime());
    }

    @Test
    void testGetRestore_Job_Pending() {
        String restoreJobId = "restore-job-pending";
        Restore restore = setupGetRestoreJobTest(restoreJobId, RestoreJobStatus.PENDING, false);

        assertEquals(RestoreStatus.RESTORING, restore.getStatus());
    }

    @Test
    void testGetException() {
        Class<? extends SubstrateSdkException> result = dbBackupRestore.getException(new RuntimeException("test"));
        assertNotNull(result);
    }

    @Test
    void testBuilder() {
        AwsDBBackupRestore.Builder builder = new AwsDBBackupRestore.Builder();
        assertNotNull(builder);

        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.build());
        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withRegion("region").build());

        // Test that building with all required fields creates an instance
        AwsDBBackupRestore instance = builder
                .withRegion("us-east-1")
                .withResourceName("my-table")
                .withBackupClient(mockBackupClient)
                .build();
        assertNotNull(instance);
        assertEquals("aws", instance.getProviderId());
    }

    @Test
    void testClose() {
        doNothing().when(mockBackupClient).close();
        dbBackupRestore.close();
        verify(mockBackupClient, times(1)).close();
    }

    @Test
    void testDefaultConstructor() {
        AwsDBBackupRestore dbrestore = new AwsDBBackupRestore();
        Assertions.assertNotNull(dbrestore);
        assertEquals("aws", dbrestore.getProviderId());

        AwsDBBackupRestore.Builder builder = dbrestore.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testBuilderCreatesDefaultClient() {
        // Test that build() creates a default BackupClient when not provided
        // This exercises the backupClient == null branch
        AwsDBBackupRestore.Builder builder = new AwsDBBackupRestore.Builder();

        // Don't provide backupClient - it should create one automatically
        AwsDBBackupRestore instance = builder
                .withRegion("us-east-1")
                .withResourceName("test-table")
                .build();

        assertNotNull(instance);
        assertEquals("aws", instance.getProviderId());
        assertEquals("us-east-1", instance.getRegion());
        assertEquals("test-table", instance.getResourceName());

        // Clean up
        instance.close();
    }
}
