package com.salesforce.multicloudj.blob.gcp;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.client.AbstractBlobStoreIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import org.junit.jupiter.api.Disabled;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Disabled("This test is disabled until we get the mocked authentication responses functioning properly")
public class GcpBlobStoreIT extends AbstractBlobStoreIT {

    private static final String endpoint = "https://storage.googleapis.com";
    private static final String serviceAccount = "chameleon@chameleon-jcloud.iam.gserviceaccount.com";
    private static final String bucketName = "chameleon-jcloud";
    private static final String versionedBucketName = "chameleon-jcloud-versioned";
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

            String tokenValue = System.getenv().getOrDefault("GOOGLE_APPLICATION_CREDENTIALS", "FAKE_ACCESS_KEY");
            if(!useValidCredentials) {
                tokenValue = "invalidToken";
            }

            String bucketNameToUse = useValidBucket ? (useVersionedBucket ? versionedBucketName : bucketName) : nonExistentBucketName;

            AccessToken accessToken = AccessToken.newBuilder()
                    .setTokenValue(tokenValue)
                    .build();
            GoogleCredentials sourceCredentials = GoogleCredentials.newBuilder()
                    .setAccessToken(accessToken)
                    .build();

            Credentials credentials = ImpersonatedCredentials.create(
                    sourceCredentials,
                    serviceAccount,
                    null,
                    List.of("https://www.googleapis.com/auth/cloud-platform"),
                    3600);

            boolean isRecordingEnabled = System.getProperty("record") != null;
            if(!isRecordingEnabled) {
                credentials = NoCredentials.getInstance();
            }

            return createBlobStore(bucketNameToUse, credentials);
        }

        private AbstractBlobStore<?> createBlobStore(final String bucketName, final Credentials credentials){

            HttpTransportOptions transportOptions = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> TestsUtilGcp.getHttpTransport(port))
                    .build();

            Storage storage = StorageOptions.newBuilder()
                    .setTransportOptions(transportOptions)
                    .setHost(getEndpoint())
                    .setCredentials(credentials)
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
