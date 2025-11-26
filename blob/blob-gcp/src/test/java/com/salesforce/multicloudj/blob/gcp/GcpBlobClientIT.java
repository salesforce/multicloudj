package com.salesforce.multicloudj.blob.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.blob.client.AbstractBlobClientIT;
import com.salesforce.multicloudj.blob.driver.AbstractBlobClient;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

import static com.salesforce.multicloudj.common.gcp.GcpConstants.PROVIDER_ID;

public class GcpBlobClientIT extends AbstractBlobClientIT {

    private static final String endpoint = "https://storage.googleapis.com";

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        Storage storage;

        @Override
        public AbstractBlobClient<?> createBlobClient(boolean useValidCredentials) {

            boolean isRecordingEnabled = System.getProperty("record") != null;

            if (isRecordingEnabled && useValidCredentials) {
                // Live recording path – rely on real ADC
                try {
                    Credentials credentials = GoogleCredentials.getApplicationDefault();
                    return createBlobClient(credentials);
                } catch (IOException e) {
                    // Fallback to NoCredentials if unable to load application default credentials
                    return createBlobClient(NoCredentials.getInstance());
                }
            } else {
                // Replay path - inject mock credentials
                GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                return createBlobClient(mockCreds);
            }
        }

        private AbstractBlobClient<?> createBlobClient(final Credentials credentials) {

            HttpTransport httpTransport = TestsUtilGcp.getHttpTransport(port);
            HttpTransportOptions transportOptions = HttpTransportOptions.newBuilder()
                    .setHttpTransportFactory(() -> httpTransport)
                    .build();

            storage = StorageOptions.newBuilder()
                    .setTransportOptions(transportOptions)
                    .setCredentials(credentials)
                    .setHost(endpoint)
                    .setProjectId("substrate-sdk-gcp-poc1")
                    .build().getService();


            GcpBlobClient.Builder builder = new GcpBlobClient.Builder();
            builder.withEndpoint(URI.create(endpoint));

            return builder.build(storage);
        }

        @Override
        public String getEndpoint() {
            return endpoint;
        }

        @Override
        public String getProviderId() {
            return PROVIDER_ID;
        }

        @Override
        public int getPort() {
            return port;
        }

        @Override
        public void close() {
            try {
                storage.close();
            } catch (Exception e) {
                throw new SubstrateSdkException("Failed to close GcpBlobClient", e);
            }
        }
    }
}
