package com.salesforce.multicloudj.dbbackuprestore.gcp;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.dbbackuprestore.client.AbstractDBBackupRestoreIT;
import com.salesforce.multicloudj.dbbackuprestore.driver.AbstractDBBackupRestore;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Integration tests for GCP Firestore DB Backup Restore implementation.
 *
 * @since 0.2.25
 */
public class FSDBBackupRestoreIT extends AbstractDBBackupRestoreIT {

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        FirestoreAdminClient firestoreAdminClient;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractDBBackupRestore createDBBackupRestoreDriver() {
            boolean isRecordingEnabled = System.getProperty("record") != null;
            // Create channel provider using transport
            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);

            // Create FirestoreAdminSettings with credentials
            FirestoreAdminSettings.Builder settingsBuilder = FirestoreAdminSettings.newBuilder();
            settingsBuilder.setTransportChannelProvider(channelProvider);
            if (!isRecordingEnabled) {
                settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
            }

            // Build the client
            try {
                firestoreAdminClient = FirestoreAdminClient.create(settingsBuilder.build());
            } catch (IOException e) {
                Assertions.fail("Failed to create the firestore admin client", e);
            }

            return new FSDBBackupRestore.Builder()
                    .withFirestoreAdminClient(firestoreAdminClient)
                    .withRegion("projects/substrate-sdk-gcp-poc1/locations/nam5")
                    .withResourceName("projects/substrate-sdk-gcp-poc1/databases/(default)/documents/docstore-test-1")
                    .build();
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public String getBackupEndpoint() {
            return "https://firestore.googleapis.com";
        }

        @Override
        public void close() {
            if (firestoreAdminClient != null) {
                firestoreAdminClient.close();
            }
        }
    }
}
