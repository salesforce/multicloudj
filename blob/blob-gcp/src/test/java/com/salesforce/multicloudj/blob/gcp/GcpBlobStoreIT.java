package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

public class GcpBlobStoreIT extends AbstractBlobStoreIT {

    private static final String endpoint = "https://storage.googleapis.com";
    private static final String bucketName = "substrate-sdk-gcp-poc1-test-bucket";
    private static final String versionedBucketName = "substrate-sdk-gcp-poc1-test-bucket-versioned";
    private static final String nonExistentBucketName = "java-bucket-does-not-exist";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        Storage storage;

        @Override
        public AbstractBlobStore<?> createBlobStore(boolean useValidBucket, boolean useValidCredentials, boolean useVersionedBucket){

            String bucketNameToUse = useValidBucket ? (useVersionedBucket ? versionedBucketName : bucketName) : nonExistentBucketName;

            boolean isRecordingEnabled = System.getProperty("record") != null;

            if (isRecordingEnabled && useValidCredentials) {
                // Live recording path – rely on real ADC
                try {
                    Credentials credentials = GoogleCredentials.getApplicationDefault();
                    return createBlobStore(bucketNameToUse, credentials);
                } catch (IOException e) {
                    // Fallback to NoCredentials if unable to load application default credentials
                    return createBlobStore(bucketNameToUse, NoCredentials.getInstance());
                }
            } else {
                // Replay path - inject mock credentials
                GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                return createBlobStore(bucketNameToUse, mockCreds);
            }
        }

        private AbstractBlobStore<?> createBlobStore(final String bucketName, final Credentials credentials){

            HttpTransport httpTransport = TestsUtilGcp.getHttpTransport(port);
            HttpTransportOptions transportOptions = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> httpTransport)
                    .build();

            Storage storage = StorageOptions.newBuilder()
                    .setTransportOptions(transportOptions)
                    .setCredentials(credentials)
                    .setHost(endpoint)
                    .build().getService();

            return new GcpBlobStore.Builder()
                    .withStorage(storage)
                    .withEndpoint(URI.create(endpoint))
                    .withBucket(bucketName)
                    .build();
        }

        @Override
        public String getEndpoint() {
            return endpoint;
        }

        @Override
        public String getProviderId() {
            return "gcp";
        }

        @Override
        public String getMetadataHeader(String key) {
            return key;     // Metadata headers don't exist in GCP
        }

        @Override
        public String getTaggingHeader() {
            return "";      // Tagging headers don't exist in GCP
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public String getKmsKeyId() {
            return "projects/chameleon-jcloud/locations/us/keyRings/chameleon-test/cryptoKeys/chameleon-test";
        }

        @Override
        public void close() {
            try {
                storage.close();
            } catch (Exception e) {
                // Burying the exception
            }
        }
    }
}