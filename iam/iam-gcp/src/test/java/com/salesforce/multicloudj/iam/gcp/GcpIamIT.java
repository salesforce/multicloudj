package com.salesforce.multicloudj.iam.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.admin.v1.IAMSettings;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.iam.client.AbstractIamIT;
import com.salesforce.multicloudj.iam.client.IamClient;
import com.salesforce.multicloudj.iam.client.TestIamClient;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Integration tests for GcpIam.
 * 
 * <p>This test class follows the conformance testing pattern used across the MultiCloudJ project.
 * 
 * <p><b>Important Note:</b> GCP IAM Admin API uses gRPC protocol, which WireMock does not
 * natively support. Therefore:
 * <ul>
 *   <li>Recording mode (-Drecord) works by connecting directly to real GCP APIs</li>
 *   <li>Replay mode is not yet implemented and will fail</li>
 *   <li>Integration tests are skipped by default in the pom.xml</li>
 * </ul>
 * 
 * <p>To run these tests, you must:
 * <ol>
 *   <li>Have valid GCP credentials configured (Application Default Credentials)</li>
 *   <li>Have access to the test project (substrate-sdk-gcp-poc1)</li>
 *   <li>Run with: {@code mvn test -DskipITs=false -Drecord -Dtest=GcpIamIT -pl iam/iam-gcp}</li>
 * </ol>
 * 
 * <p>Future work: Implement gRPC mocking using grpc-mock, in-process gRPC server,
 * or a gRPC-aware proxy solution.
 */
public class GcpIamIT extends AbstractIamIT {
    
    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }
    
    /**
     * Harness implementation for GCP IAM integration tests.
     */
    public static class HarnessImpl implements AbstractIamIT.Harness {
        IAMClient client;
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        
        private static final String TEST_PROJECT_ID = "substrate-sdk-gcp-poc1";
        private static final String TEST_REGION = "us-central1";
        private static final String TEST_IDENTITY_NAME = "test-multicloudj-sa";
        private static final String IAM_ENDPOINT = "https://iam.googleapis.com";
        private static final String TRUSTED_PRINCIPAL = "chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";
        
        @Override
        public AbstractIam createIamDriver() {
            boolean isRecordingEnabled = System.getProperty("record") != null;
            
            try {
                if (isRecordingEnabled) {
                    // Live recording path – rely on real ADC
                    // Note: GCP IAM Admin API uses gRPC, which WireMock doesn't support natively.
                    // For recording, we connect directly to the real GCP API.
                    client = IAMClient.create();
                    return new GcpIam(new GcpIam.Builder().withIamClient(client));
                } else {
                    // Replay path - inject mock credentials
                    // Note: Since WireMock doesn't support gRPC, replay mode is not yet implemented.
                    // This will fail with "Transport not supported: httpjson" error.
                    // TODO: Implement gRPC mocking solution (e.g., using grpc-mock or in-process server)
                    GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                    IAMSettings settings = IAMSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(mockCreds))
                            .build();
                    client = IAMClient.create(settings);
                    return new GcpIam(new GcpIam.Builder().withIamClient(client));
                }
            } catch (IOException e) {
                Assertions.fail("Failed to create IAM client", e);
                return null;
            }
        }
        
        @Override
        public IamClient createIamClient() {
            AbstractIam driver = createIamDriver();
            return TestIamClient.create(driver);
        }
        
        @Override
        public String getTenantId() {
            return TEST_PROJECT_ID;
        }
        
        @Override
        public String getRegion() {
            return TEST_REGION;
        }
        
        @Override
        public String getTestIdentityName() {
            return TEST_IDENTITY_NAME;
        }
        
        @Override
        public String getIamEndpoint() {
            return IAM_ENDPOINT;
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
        public boolean supportsCreateIdentity() {
            return true;
        }
        
        @Override
        public boolean supportsCreateIdentityWithTrustConfig() {
            return true;
        }
        
        @Override
        public boolean supportsGetIdentity() {
            return true;
        }
        
        @Override
        public boolean supportsDeleteIdentity() {
            return false; // Not yet implemented
        }
        
        @Override
        public boolean supportsAttachInlinePolicy() {
            return false; // Not yet implemented
        }
        
        @Override
        public boolean supportsGetInlinePolicyDetails() {
            return false; // Not yet implemented
        }
        
        @Override
        public boolean supportsGetAttachedPolicies() {
            return false; // Not yet implemented
        }
        
        @Override
        public boolean supportsRemovePolicy() {
            return false; // Not yet implemented
        }
        
        @Override
        public String getTrustedPrincipal() {
            return TRUSTED_PRINCIPAL;
        }
        
        @Override
        public void close() {
            if (client != null) {
                client.close();
            }
        }
    }
}
