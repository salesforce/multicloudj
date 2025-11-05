package com.salesforce.multicloudj.sts.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.sts.client.AbstractStsIT;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
public class GcpStsIT extends AbstractStsIT {
    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements AbstractStsIT.Harness {
        IamCredentialsClient client;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        @Override
        public AbstractSts createStsDriver(boolean longTermCredentials) {
            boolean isRecordingEnabled = System.getProperty("record") != null;
            // Transport channel provider to WireMock proxy
            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);
            IamCredentialsSettings.Builder settingsBuilder = IamCredentialsSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider);
            try {
                if (isRecordingEnabled) {
                    // Live recording path – rely on real ADC
                    client = IamCredentialsClient.create(settingsBuilder.build());
                    return new GcpSts().builder().build(client);
                } else {
                    // Replay path - inject mock credentials
                    GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                    settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(mockCreds));
                    client = IamCredentialsClient.create(settingsBuilder.build());
                    return new GcpSts().builder().build(client, mockCreds);
                }
            } catch (IOException e) {
                Assertions.fail("Failed to create IAM client", e);
                return null;
            }
        }

        @Override
        public String getRoleName() {
            return "chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";
        }

        @Override
        public String getStsEndpoint() {
            return "https://iamcredentials.googleapis.com";
        }

        @Override
        public String getProviderId() {
            return GcpConstants.PROVIDER_ID;
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
        public boolean supportsGetAccessToken() {
            return true;
        }

        @Override
        public void close() {
            client.close();
        }
    }
}
