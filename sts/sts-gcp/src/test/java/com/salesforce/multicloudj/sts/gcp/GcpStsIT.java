package com.salesforce.multicloudj.sts.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.sts.client.AbstractStsIT;
import com.salesforce.multicloudj.sts.driver.AbstractSts;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
public class GcpStsIT extends AbstractStsIT {
    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }

    public static class HarnessImpl implements AbstractStsIT.Harness {
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);

        @Override
        public AbstractSts createStsDriver(boolean longTermCredentials) {
            boolean isRecordingEnabled = System.getProperty("record") != null;

            HttpTransport httpTransport = TestsUtilGcp.getHttpTransport(port);
            HttpTransportFactory httpTransportFactory = () -> httpTransport;

            if (isRecordingEnabled) {
                return new GcpSts().builder().build(httpTransportFactory);
            } else {
                GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                return new GcpSts().builder().build(mockCreds, httpTransportFactory);
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
            // No client to close
        }
    }
}
