package com.salesforce.multicloudj.iam.gcp;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.admin.v1.IAMSettings;
import com.google.cloud.iam.admin.v1.stub.IAMStubSettings;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import com.google.cloud.resourcemanager.v3.ProjectsSettings;
import com.salesforce.multicloudj.common.gcp.util.MockGoogleCredentialsFactory;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.common.gcp.util.TestsUtilGcp;
import com.salesforce.multicloudj.iam.client.AbstractIamIT;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.GrpcExtensionFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.salesforce.multicloudj.common.util.common.WiremockGrpcUtil.getGrpcTransportChannelWithRecordingInterceptor;
import static com.salesforce.multicloudj.common.util.common.WiremockGrpcUtil.initializeWireMockGrpcService;
import static com.salesforce.multicloudj.common.util.common.WiremockGrpcUtil.saveProtoDescriptor;

/**
 * Integration tests for GcpIam with gRPC record/replay support.
 *
 * <p>NOTE: GCP IAM Admin API is gRPC-only. Unlike other conformance tests that use
 * HTTP/HTTPS proxy-based WireMock recording, this test uses:
 * <ul>
 *   <li><b>RECORD mode:</b> Wraps IAMClient to intercept and save responses as JSON files</li>
 *   <li><b>REPLAY mode:</b> Loads saved responses and registers them as WireMock gRPC stubs</li>
 * </ul>
 * 
 * <p>The test automatically generates proto descriptors and manages WireMock gRPC lifecycle.
 */
public class GcpIamIT extends AbstractIamIT {

    private static final Logger logger = LoggerFactory.getLogger(GcpIamIT.class);
    private static WireMockServer grpcWireMockServer;
    private static final String GRPC_DIR = "src/test/resources/grpc";
    private static final String RECORDINGS_DIR = "src/test/resources/recordings";
    
    /**
     * Sets up WireMock server with gRPC extension for replay mode.
     * Synchronized to prevent race conditions if called from multiple test instances.
     */
    private static synchronized void setupGrpcWireMock(int port) throws IOException {
        // Double-check if already set up
        if (grpcWireMockServer != null && grpcWireMockServer.isRunning()) {
            return;
        }

        // Generate proto descriptor if not present already
        // saveProtoDescriptor(ServiceAccount.getDescriptor().getFile(), GRPC_DIR, "iam_admin.dsc");
        
        // Start WireMock with gRPC Extension (plaintext)
        WireMockConfiguration config = WireMockConfiguration.wireMockConfig()
                .port(port)
                .extensions(new GrpcExtensionFactory());
        
        grpcWireMockServer = new WireMockServer(config);
        grpcWireMockServer.start();
        initializeWireMockGrpcService(port, "google.iam.admin.v1.IAM", RECORDINGS_DIR);
    }

    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }
    
    /**
     * Harness implementation for GCP IAM integration tests with gRPC WireMock support.
     */
    public static class HarnessImpl implements AbstractIamIT.Harness {

        IAMClient iamClient;
        ProjectsClient projectsClient;
        // Parent starts HTTP WireMock on this port, we use a different port for gRPC
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        int grpcPort = ThreadLocalRandom.current().nextInt(10000, 20000);

        private static final String TEST_PROJECT_ID = "projects/substrate-sdk-gcp-poc1";
        private static final String TEST_REGION = "us-west1";
        private static final String TEST_IDENTITY_NAME = "testSa";
        private static final String IAM_ENDPOINT = "https://cloudresourcemanager.googleapis.com";
        private static final String TRUSTED_PRINCIPAL = "chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";

        @Override
        public AbstractIam createIamDriver() {
            boolean isRecordMode = System.getProperty("record") != null;
            TransportChannelProvider channelProvider = TestsUtilGcp.getTransportChannelProvider(port);
            ProjectsSettings.Builder projectsSettingsBuilder = ProjectsSettings.newBuilder()
                    .setTransportChannelProvider(channelProvider);
            try {
                if (isRecordMode) {
                    
                    IAMStubSettings.Builder stubSettingsBuilder = IAMStubSettings.newBuilder();
                    stubSettingsBuilder.setTransportChannelProvider(
                        getGrpcTransportChannelWithRecordingInterceptor(RECORDINGS_DIR)
                    );
                    
                    iamClient = IAMClient.create(IAMSettings.create(stubSettingsBuilder.build()));
                    projectsClient = ProjectsClient.create(projectsSettingsBuilder.build());
                    return new GcpIam.Builder()
                            .withProjectsClient(projectsClient)
                            .withIamClient(iamClient)
                            .build();
                } else {
                    // REPLAY mode: Setup gRPC WireMock if not already done, then connect
                    logger.info("--- REPLAY MODE: Connecting to WireMock gRPC ---");
                    
                    // Lazy initialization of gRPC WireMock
                    if (grpcWireMockServer == null || !grpcWireMockServer.isRunning()) {
                        setupGrpcWireMock(grpcPort);
                    }
                    
                    IAMStubSettings.Builder stubSettingsBuilder = IAMStubSettings.newBuilder();
                    stubSettingsBuilder.setEndpoint("localhost:" + grpcPort);
                    stubSettingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
                    
                    // Use plaintext for WireMock gRPC connections
                    stubSettingsBuilder.setTransportChannelProvider(
                        InstantiatingGrpcChannelProvider.newBuilder()
                            .setChannelConfigurator(io.grpc.ManagedChannelBuilder::usePlaintext)
                            .build()
                    );
                    
                    iamClient = IAMClient.create(IAMSettings.create(stubSettingsBuilder.build()));
                    GoogleCredentials mockCreds = MockGoogleCredentialsFactory.createMockCredentials();
                    projectsSettingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(mockCreds));
                    projectsClient = ProjectsClient.create(projectsSettingsBuilder.build());
                    return new GcpIam.Builder()
                            .withProjectsClient(projectsClient)
                            .withIamClient(iamClient)
                            .build();
                }
            } catch (IOException e) {
                Assertions.fail("Failed to create IAM client", e);
                return null;
            }
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
            // Port is used by parent class to start WireMock server
            // for HTTP-based policy API recording via WireMock proxy.
            return port;
        }
        
        @Override
        public List<String> getWiremockExtensions() {
            // HTTP-based policy APIs use JSON transformer for record/replay.
            return List.of("com.salesforce.multicloudj.iam.gcp.util.IamJsonResponseTransformer");
        }
        
        @Override
        public String getTrustedPrincipal() {
            return TRUSTED_PRINCIPAL;
        }

        @Override
        public String getIdentityName() {
            return "serviceAccount:chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";
        }

        @Override
        public String getTestPolicyEffect() {
            return "Allow";
        }

        @Override
        public List<String> getTestPolicyActions() {
            return List.of("roles/storage.objectViewer", "roles/storage.objectCreator");
        }

        @Override
        public String getTestPolicyName() {
            return "roles/storage.objectViewer";
        }

        @Override
        public void close() {
            if (iamClient != null) {
                iamClient.close();
            }
            if (projectsClient != null) {
                projectsClient.close();
            }
            // Clean up gRPC WireMock server if it's running
            if (grpcWireMockServer != null && grpcWireMockServer.isRunning()) {
                grpcWireMockServer.stop();
                logger.info("Stopped gRPC WireMock server");
            }
        }
    }
}
