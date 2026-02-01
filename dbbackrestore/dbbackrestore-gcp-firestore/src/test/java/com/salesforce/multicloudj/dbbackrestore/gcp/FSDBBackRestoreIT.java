package com.salesforce.multicloudj.dbbackrestore.gcp;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.cloud.firestore.v1.FirestoreAdminSettings;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.dbbackrestore.client.AbstractDBBackRestoreIT;
import com.salesforce.multicloudj.dbbackrestore.driver.AbstractDBBackRestore;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Assertions;

/**
 * Integration tests for GCP Firestore DB Backup Restore implementation.
 *
 * @since 0.2.25
 */
public class FSDBBackRestoreIT extends AbstractDBBackRestoreIT {

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        FirestoreAdminClient firestoreAdminClient;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractDBBackRestore createDBBackRestoreDriver() {
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

            return new FSDBBackRestore.Builder()
                    .withFirestoreAdminClient(firestoreAdminClient)
                    .withRegion("projects/substrate-sdk-gcp-poc1/nam5")
                    .withResourceName("projects/substrate-sdk-gcp-poc1/databases/(default)/documents/docstore-test-1")
                    .withProjectId("substrate-sdk-gcp-poc1")
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
