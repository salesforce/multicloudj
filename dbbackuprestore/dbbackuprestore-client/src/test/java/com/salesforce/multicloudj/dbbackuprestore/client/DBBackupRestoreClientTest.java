package com.salesforce.multicloudj.dbbackuprestore.client;

import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.BackupStatus;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the DBBackupRestoreClient class.
 */
@ExtendWith(MockitoExtension.class)
public class DBBackupRestoreClientTest {

    @Mock
    private AbstractDBBackupRestore mockDriver;

    private DBBackupRestoreClient client;

    @BeforeEach
    void setUp() {
        client = new DBBackupRestoreClient(mockDriver);
    }

    @Test
    void testBuilderWithTestProvider() {
        TestConcreteAbstractDBBackupRestore provider = new TestConcreteAbstractDBBackupRestore();

        // Mock the ServiceLoader to return test provider
        ServiceLoader<AbstractDBBackupRestore> serviceLoader = mock(ServiceLoader.class);
        Iterator<AbstractDBBackupRestore> providerIterator =
                List.<AbstractDBBackupRestore>of(provider).iterator();
        when(serviceLoader.iterator()).thenReturn(providerIterator);

        try (MockedStatic<ServiceLoader> serviceLoaderStatic = Mockito.mockStatic(ServiceLoader.class)) {
            serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractDBBackupRestore.class))
                    .thenReturn(serviceLoader);

            // Test builder methods
            DBBackupRestoreClient.DBBackupRestoreClientBuilder builder =
                    DBBackupRestoreClient.builder("mockProviderId");
            builder.withRegion("us-west-2");
            builder.withResourceName("test-database");

            DBBackupRestoreClient client = builder.build();

            // Assertions - The client should be built successfully
            assertNotNull(client);
            // The provider ID should match
            assertEquals("mockProviderId", provider.getProviderId());
        }
    }

    @Test
    void testListBackups() {
        List<Backup> expectedBackups =
                Arrays.asList(
                        Backup.builder()
                                .backupId("backup-1")
                                .resourceName("table-1")
                                .status(BackupStatus.AVAILABLE)
                                .build(),
                        Backup.builder()
                                .backupId("backup-2")
                                .resourceName("table-1")
                                .status(BackupStatus.CREATING)
                                .build());

        when(mockDriver.listBackups()).thenReturn(expectedBackups);

        List<Backup> actualBackups = client.listBackups();

        assertEquals(2, actualBackups.size());
        assertEquals("backup-1", actualBackups.get(0).getBackupId());
        assertEquals("backup-2", actualBackups.get(1).getBackupId());
        verify(mockDriver, times(1)).listBackups();
    }

    @Test
    void testListBackupsEmpty() {
        when(mockDriver.listBackups()).thenReturn(Collections.emptyList());

        List<Backup> actualBackups = client.listBackups();

        assertEquals(0, actualBackups.size());
        verify(mockDriver, times(1)).listBackups();
    }

    @Test
    void testGetBackup() {
        Backup expectedBackup =
                Backup.builder()
                        .backupId("backup-123")
                        .resourceName("test-table")
                        .status(BackupStatus.AVAILABLE)
                        .creationTime(Instant.now())
                        .sizeInBytes(1024L)
                        .build();

        when(mockDriver.getBackup("backup-123")).thenReturn(expectedBackup);

        Backup actualBackup = client.getBackup("backup-123");

        assertNotNull(actualBackup);
        assertEquals("backup-123", actualBackup.getBackupId());
        assertEquals("test-table", actualBackup.getResourceName());
        assertEquals(BackupStatus.AVAILABLE, actualBackup.getStatus());
        assertEquals(1024L, actualBackup.getSizeInBytes());
        verify(mockDriver, times(1)).getBackup("backup-123");
    }

    @Test
    void testRestoreBackup() {
        RestoreRequest request =
                RestoreRequest.builder()
                        .backupId("backup-789")
                        .targetResource("restored-table")
                        .vaultId("vault-abc")
                        .roleId("role-123")
                        .build();

        when(mockDriver.restoreBackup(any())).thenReturn("test");
        client.restoreBackup(request);
        verify(mockDriver, times(1)).restoreBackup(request);
    }

    @Test
    void testClose() throws Exception {
        doNothing().when(mockDriver).close();
        client.close();
        verify(mockDriver, times(1)).close();
    }

    @Test
    void testBuilderInvalidProviderId() {
        // This will fail at runtime when ServiceLoader can't find the provider
        assertThrows(
                IllegalArgumentException.class,
                () -> DBBackupRestoreClient.builder("invalid-provider-xyz")
                        .withRegion("us-west-2")
                        .withResourceName("test-table")
                        .build());
    }
}
