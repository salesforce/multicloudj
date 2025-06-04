package com.salesforce.multicloudj.sts.gcp;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
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
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        IamCredentialsClient client;
        @Override
        public AbstractSts<?> createStsDriver(boolean longTermCredentials) {
            boolean isRecordingEnabled = System.getProperty("record") != null;
            // Create channel provider using transport
            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);
            // Create IamCredentialsSettings with credentials
            IamCredentialsSettings.Builder settingsBuilder = IamCredentialsSettings.newBuilder();
            settingsBuilder.setTransportChannelProvider(channelProvider);
            if (!isRecordingEnabled) {
                settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
            }
            // Build the client
            try {
                this.client = IamCredentialsClient.create(settingsBuilder.build());
            } catch (IOException e) {
                Assertions.fail("Failed to create the IAM client", e);
            }
            return new GcpSts().builder().build(client);
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
            return List.of("");
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
