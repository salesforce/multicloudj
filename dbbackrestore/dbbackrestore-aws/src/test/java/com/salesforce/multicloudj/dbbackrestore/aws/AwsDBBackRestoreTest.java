package com.salesforce.multicloudj.dbbackrestore.aws;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
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
import software.amazon.awssdk.services.backup.BackupClient;
import software.amazon.awssdk.services.backup.model.CalculatedLifecycle;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointRequest;
import software.amazon.awssdk.services.backup.model.DescribeRecoveryPointResponse;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceRequest;
import software.amazon.awssdk.services.backup.model.ListRecoveryPointsByResourceResponse;
import software.amazon.awssdk.services.backup.model.RecoveryPointByResource;
import software.amazon.awssdk.services.backup.model.RecoveryPointStatus;
import software.amazon.awssdk.services.backup.model.StartRestoreJobRequest;
import software.amazon.awssdk.services.backup.model.StartRestoreJobResponse;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AwsDBBackRestore.
 */
@ExtendWith(MockitoExtension.class)
public class AwsDBBackRestoreTest {

    @Mock
    private BackupClient mockBackupClient;

    private AwsDBBackRestore dbBackRestore;
    private static final String TABLE_ARN = "arn:aws:dynamodb:us-west-2:123456789012:table/test-table";
    private static final String IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/RestoreRole";

    @BeforeEach
    void setUp() {
        dbBackRestore = new AwsDBBackRestore.Builder()
                .withBackupClient(mockBackupClient)
                .withRegion("us-west-2")
                .withResourceName("test-table")
                .withTableArn(TABLE_ARN)
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

        List<Backup> backups = dbBackRestore.listBackups();

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

        Backup backup = dbBackRestore.getBackup("arn:aws:backup:us-west-2:123456789012:recovery-point:123");

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
    void testGetBackupStatus() {
        // Mock listRecoveryPointsByResource to find the vault
        RecoveryPointByResource recovery = RecoveryPointByResource.builder()
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:456")
                .backupVaultName("Default")
                .backupSizeBytes(512L)
                .build();

        ListRecoveryPointsByResourceResponse listResponse = ListRecoveryPointsByResourceResponse.builder()
                .recoveryPoints(Arrays.asList(recovery))
                .build();

        when(mockBackupClient.listRecoveryPointsByResource(any(ListRecoveryPointsByResourceRequest.class)))
                .thenReturn(listResponse);

        DescribeRecoveryPointResponse response = DescribeRecoveryPointResponse.builder()
                .recoveryPointArn("arn:aws:backup:us-west-2:123456789012:recovery-point:456")
                .backupVaultName("Default")
                .status(RecoveryPointStatus.CREATING)
                .creationDate(Instant.now())
                .build();

        when(mockBackupClient.describeRecoveryPoint(any(DescribeRecoveryPointRequest.class)))
                .thenReturn(response);

        BackupStatus status = dbBackRestore.getBackupStatus("arn:aws:backup:us-west-2:123456789012:recovery-point:456");

        assertEquals(BackupStatus.CREATING, status);
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

        dbBackRestore.restoreBackup(request);

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
        Assertions.assertThrows(IllegalArgumentException.class, () -> dbBackRestore.restoreBackup(request));
    }

    @Test
    void testGetException() {
        Class<? extends SubstrateSdkException> result = dbBackRestore.getException(new RuntimeException("test"));
        assertNotNull(result);
    }

    @Test
    void testBuilder() {
        AwsDBBackRestore.Builder builder = new AwsDBBackRestore.Builder();
        assertNotNull(builder);

        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.build());
        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withRegion("region").build());

        // Test that building with all required fields creates an instance
        AwsDBBackRestore instance = builder
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
        dbBackRestore.close();
        verify(mockBackupClient, times(1)).close();
    }

    @Test
    void testDefaultConstructor() {
        AwsDBBackRestore dbrestore = new AwsDBBackRestore();
        Assertions.assertNotNull(dbrestore);
        assertEquals("aws", dbrestore.getProviderId());

        AwsDBBackRestore.Builder builder = dbrestore.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testBuilderCreatesDefaultClient() {
        // Test that build() creates a default BackupClient when not provided
        // This exercises the backupClient == null branch
        AwsDBBackRestore.Builder builder = new AwsDBBackRestore.Builder();

        // Don't provide backupClient - it should create one automatically
        AwsDBBackRestore instance = builder
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
