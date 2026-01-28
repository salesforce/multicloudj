package com.salesforce.multicloudj.docstore.gcp;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.firestore.v1.FirestoreClient;
import com.google.cloud.firestore.v1.FirestoreSettings;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.docstore.client.AbstractDocstoreIT;
import com.salesforce.multicloudj.docstore.client.CollectionKind;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FSDocstoreIT extends AbstractDocstoreIT {

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        FirestoreClient firestoreClient;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractDocStore createDocstoreDriver(CollectionKind collectionKind) {
            boolean isRecordingEnabled = System.getProperty("record") != null;
            // Create channel provider using transport
            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);
            // Create FirestoreSettings with credentials
            FirestoreSettings.Builder settingsBuilder = FirestoreSettings.newBuilder();
            settingsBuilder.setTransportChannelProvider(channelProvider);
            if (!isRecordingEnabled) {
                settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
            }

            // Build the client
            try {
                firestoreClient = FirestoreClient.create(settingsBuilder.build());
            } catch (IOException e) {
                Assertions.fail("Failed to create the firestore client", e);
            }

            CollectionOptions collectionOptions = null;
            if (collectionKind == CollectionKind.SINGLE_KEY) {
                collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName("projects/substrate-sdk-gcp-poc1/databases/(default)/documents/docstore-test-1")
                        .withPartitionKey("pName")
                        .build();
            } else if (collectionKind == CollectionKind.TWO_KEYS) {
                collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                        .withTableName("projects/substrate-sdk-gcp-poc1/databases/(default)/documents/docstore-test-2")
                        .withPartitionKey("Game")
                        .withSortKey("Player")
                        .build();
            }

            return new FSDocStore().builder()
                    .withFirestoreV1Client(firestoreClient)
                    .withCollectionOptions(collectionOptions)
                    .build();
        }

        @Override
        public Object getRevisionId() {
            return Timestamp.newBuilder().setSeconds(Math.abs(123)).build();
        }

        @Override
        public String getDocstoreEndpoint() {
            return "https://firestore.googleapis.com";
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public List<String> getWiremockExtensions() {
            return List.of();
        }

        @Override
        public boolean supportOrderByInFullScan() {
            return true;
        }

        @Override
        public boolean supportsBackupCreation() {
            return true;
        }

        @Override
        public boolean supportsBackupListing() {
            return true;
        }

        @Override
        public boolean supportsBackupRestore() {
            return true;
        }

        @Override
        public void close() {
            if (firestoreClient != null) {
                firestoreClient.close();
            }
        }
    }
} 