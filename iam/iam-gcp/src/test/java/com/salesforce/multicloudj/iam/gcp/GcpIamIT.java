package com.salesforce.multicloudj.iam.gcp;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.admin.v1.IAMSettings;
import com.google.cloud.iam.admin.v1.stub.IAMStubSettings;
import com.google.iam.admin.v1.ServiceAccount;
import com.google.iam.v1.Policy;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.iam.client.AbstractIamIT;
import com.salesforce.multicloudj.iam.client.IamClient;
import com.salesforce.multicloudj.iam.client.TestIamClient;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wiremock.grpc.GrpcExtensionFactory;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.wiremock.grpc.dsl.WireMockGrpc.message;
import static org.wiremock.grpc.dsl.WireMockGrpc.method;

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
 * 
 * <p><b>Usage:</b>
 * <ul>
 *   <li>Record mode: {@code mvn test -Drecord} - Saves responses to {@code src/test/resources/recordings/iam/}</li>
 *   <li>Replay mode: {@code mvn test} - Loads stubs from recordings directory</li>
 * </ul>
 */
public class GcpIamIT extends AbstractIamIT {

    private static final Logger logger = LoggerFactory.getLogger(GcpIamIT.class);
    private static WireMockServer grpcWireMockServer;
    private static WireMockGrpcService mockIamService;
    private static final String GRPC_DIR = "src/test/resources/grpc";
    private static final String RECORDINGS_DIR = "src/test/resources/recordings/iam";
    
    /**
     * Override parent's test setup - gRPC doesn't use HTTP proxy recording.
     */
    @Override
    @BeforeEach
    public void setupTestEnvironment() {
        // Don't call super - gRPC doesn't use WireMock HTTP proxy recording
    }
    
    /**
     * Override parent's test cleanup - gRPC doesn't use HTTP proxy recording.
     */
    @Override
    @AfterEach
    public void cleanupTestEnvironment() {
        // Don't call super - gRPC doesn't use WireMock HTTP proxy recording
    }
    
    /**
     * Sets up WireMock server with gRPC extension for replay mode.
     * Synchronized to prevent race conditions if called from multiple test instances.
     */
    private static synchronized void setupGrpcWireMock(int port) throws IOException {
        // Double-check if already set up
        if (grpcWireMockServer != null && grpcWireMockServer.isRunning()) {
            return;
        }
        // 1. Prepare directories and clean up old descriptor files
        Files.createDirectories(Paths.get(GRPC_DIR));
        
        // Delete any existing descriptor files to avoid corruption
        Path grpcDir = Paths.get(GRPC_DIR);
        if (Files.exists(grpcDir)) {
            Files.list(grpcDir)
                .filter(path -> path.toString().endsWith(".dsc"))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                        logger.info("Deleted old descriptor: {}", path);
                    } catch (IOException e) {
                        logger.error("Failed to delete: {}", path, e);
                    }
                });
        }
        
        // 2. Generate proto descriptor BEFORE starting WireMock
        saveProtoDescriptor();
        
        // 3. Start WireMock with gRPC Extension (plaintext)
        WireMockConfiguration config = WireMockConfiguration.wireMockConfig()
                .port(port)
                .extensions(new GrpcExtensionFactory());
        
        grpcWireMockServer = new WireMockServer(config);
        grpcWireMockServer.start();
        
        logger.info("gRPC WireMock started on port: {}", port);
        
        // 4. Initialize WireMockGrpcService for the IAM service
        mockIamService = new WireMockGrpcService(
            new WireMock(port),
            "google.iam.admin.v1.IAM"
        );
        logger.info("Initialized WireMockGrpcService for google.iam.admin.v1.IAM");
        
        // 5. Load stubs from recorded JSON files
        loadAllStubs();
    }
    
    /**
     * Loads all stub files from the recordings directory.
     */
    private static void loadAllStubs() throws IOException {
        Path recordingsDir = Paths.get(RECORDINGS_DIR);
        if (!Files.exists(recordingsDir)) {
            logger.info("No recordings directory found: {}", recordingsDir);
            logger.info("Run tests with -Drecord first to create recordings");
            return;
        }
        
        int stubsLoaded = 0;
        int stubsFailed = 0;
        
        // Load all JSON files as stubs
        try (Stream<Path> paths = Files.walk(recordingsDir)) {
            List<Path> jsonFiles = paths.filter(Files::isRegularFile)
                                        .filter(path -> path.toString().endsWith(".json"))
                                        .collect(java.util.stream.Collectors.toList());
            
            logger.info("Found {} recording files", jsonFiles.size());
            
            for (Path path : jsonFiles) {
                try {
                    loadStubFromFile(path);
                    stubsLoaded++;
                } catch (IOException e) {
                    stubsFailed++;
                    logger.error("Failed to load stub: {}", path, e);
                }
            }
        }
        
        logger.info("Successfully loaded {} stubs, {} failed", stubsLoaded, stubsFailed);
        logger.info("Total stub mappings in WireMock: {}", grpcWireMockServer.getStubMappings().size());
    }
    
    /**
     * Loads a single stub file and registers it with WireMock.
     * File naming convention: <MethodName>-<uniqueId>.json
     */
    private static void loadStubFromFile(Path filePath) throws IOException {
        String fileName = filePath.getFileName().toString();
        
        // Extract method name from filename (format: MethodName-uniqueId.json)
        String methodName = extractMethodName(fileName);
        if (methodName == null) {
            logger.info("Skipping file with unexpected format: {}", fileName);
            return;
        }
        
        // Read the response JSON
        String jsonContent = new String(Files.readAllBytes(filePath));
        
        logger.info("Processing stub file: {}", fileName);
        logger.info("  Extracted method name: {}", methodName);
        logger.info("  JSON content length: {} bytes", jsonContent.length());
        
        // Determine the response type based on method name and parse accordingly
        Message responseMessage = parseResponseForMethod(methodName, jsonContent);
        if (responseMessage == null) {
            logger.error("  ERROR: Unknown method type, skipping: {}", methodName);
            return;
        }
        
        logger.info("  Parsed message type: {}", responseMessage.getClass().getSimpleName());
        
        // Register stub with WireMock
        mockIamService.stubFor(
            method(methodName)
                .willReturn(message(responseMessage))
        );
        
        logger.info("  ✓ Successfully registered stub for: {}", methodName);
    }
    
    /**
     * Extracts method name from filename.
     * Filenames are in format: MethodName-uniqueId.json or MethodName.json
     */
    private static String extractMethodName(String fileName) {
        if (!fileName.endsWith(".json")) {
            return null;
        }
        
        // Remove .json extension
        String nameWithoutExt = fileName.substring(0, fileName.length() - 5);
        
        // Handle both formats: "CreateServiceAccount-abc123.json" and "CreateServiceAccount.json"
        int dashIndex = nameWithoutExt.indexOf('-');
        if (dashIndex > 0) {
            return nameWithoutExt.substring(0, dashIndex);
        }
        
        return nameWithoutExt;
    }
    
    /**
     * Parses response JSON into the appropriate protobuf message type.
     */
    private static Message parseResponseForMethod(String methodName, String jsonContent) throws IOException {
        try {
            switch (methodName) {
                case "CreateServiceAccount":
                case "GetServiceAccount":
                    ServiceAccount.Builder saBuilder = ServiceAccount.newBuilder();
                    JsonFormat.parser().merge(jsonContent, saBuilder);
                    return saBuilder.build();
                    
                case "DeleteServiceAccount":
                    // DeleteServiceAccount returns Empty
                    Empty.Builder emptyBuilder = Empty.newBuilder();
                    JsonFormat.parser().merge(jsonContent, emptyBuilder);
                    return emptyBuilder.build();
                    
                case "GetIamPolicy":
                case "SetIamPolicy":
                    // IAM Policy methods return Policy
                    Policy.Builder policyBuilder = Policy.newBuilder();
                    JsonFormat.parser().merge(jsonContent, policyBuilder);
                    return policyBuilder.build();
                    
                default:
                    logger.warn("Unknown method type: {}", methodName);
                    return null;
            }
        } catch (Exception e) {
            throw new IOException("Failed to parse response for " + methodName, e);
        }
    }
    
    /**
     * Saves the proto descriptor file that WireMock needs to parse gRPC requests.
     */
    private static void saveProtoDescriptor() throws IOException {
        Descriptors.FileDescriptor fileDescriptor = ServiceAccount.getDescriptor().getFile();
        
        DescriptorProtos.FileDescriptorSet.Builder descriptorSetBuilder = 
            DescriptorProtos.FileDescriptorSet.newBuilder();
        
        Set<String> addedFiles = new HashSet<>();
        addFileDescriptorWithDependencies(fileDescriptor, descriptorSetBuilder, addedFiles);
        
        DescriptorProtos.FileDescriptorSet descriptorSet = descriptorSetBuilder.build();
        
        Path descriptorPath = Paths.get(GRPC_DIR, "iam_admin.dsc");
        Files.write(descriptorPath, descriptorSet.toByteArray());
        
        logger.info("Proto descriptor saved: {}", descriptorPath.toAbsolutePath());
        logger.info("Descriptor contains {} files", descriptorSet.getFileCount());
    }
    
    /**
     * Recursively adds file descriptor and dependencies.
     */
    private static void addFileDescriptorWithDependencies(
            Descriptors.FileDescriptor fileDescriptor,
            DescriptorProtos.FileDescriptorSet.Builder builder,
            Set<String> addedFiles) {
        
        String fileName = fileDescriptor.getName();
        if (addedFiles.contains(fileName)) {
            return;
        }
        
        for (Descriptors.FileDescriptor dependency : fileDescriptor.getDependencies()) {
            addFileDescriptorWithDependencies(dependency, builder, addedFiles);
        }
        
        builder.addFile(fileDescriptor.toProto());
        addedFiles.add(fileName);
    }
    
    @Override
    protected Harness createHarness() {
        return new HarnessImpl();
    }
    
    /**
     * Harness implementation for GCP IAM integration tests with gRPC WireMock support.
     */
    public static class HarnessImpl implements AbstractIamIT.Harness {
        IAMClient client;
        // Parent starts HTTP WireMock on this port, we use a different port for gRPC
        int port = ThreadLocalRandom.current().nextInt(1000, 10000);
        int grpcPort = ThreadLocalRandom.current().nextInt(10000, 20000);
        
        private static final String TEST_PROJECT_ID = "substrate-sdk-gcp-poc1";
        private static final String TEST_REGION = "us-central1";
        private static final String TEST_IDENTITY_NAME = "test-sa";
        private static final String IAM_ENDPOINT = "https://iam.googleapis.com";
        private static final String TRUSTED_PRINCIPAL = "chameleon@substrate-sdk-gcp-poc1.iam.gserviceaccount.com";
        
        @Override
        public AbstractIam createIamDriver() {
            boolean isRecordMode = System.getProperty("record") != null;
            
            try {
                if (isRecordMode) {
                    // RECORD mode: Use gRPC interceptor to capture and save responses
                    logger.info("--- RECORD MODE: Connecting to Real GCP with Recording ---");
                    Files.createDirectories(Paths.get(RECORDINGS_DIR));
                    
                    // Create IAM client with recording interceptor
                    RecordingInterceptor recordingInterceptor = new RecordingInterceptor();
                    
                    IAMStubSettings.Builder stubSettingsBuilder = IAMStubSettings.newBuilder();
                    stubSettingsBuilder.setTransportChannelProvider(
                        InstantiatingGrpcChannelProvider.newBuilder()
                            .setInterceptorProvider(() -> List.of(recordingInterceptor))
                            .build()
                    );
                    
                    client = IAMClient.create(IAMSettings.create(stubSettingsBuilder.build()));
                    return new GcpIam(new GcpIam.Builder().withIamClient(client));
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
                    
                    client = IAMClient.create(IAMSettings.create(stubSettingsBuilder.build()));
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
            // Port is used by parent class to start WireMock server
            // (even though we don't use it for gRPC - it just sits idle)
            return port;
        }
        
        @Override
        public List<String> getWiremockExtensions() {
            // Not applicable - gRPC APIs can't use WireMock's HTTP proxy mode
            // Returning empty list as WireMock is not used for gRPC testing
            return List.of();
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
            // Clean up gRPC WireMock server if it's running
            if (grpcWireMockServer != null && grpcWireMockServer.isRunning()) {
                grpcWireMockServer.stop();
                logger.info("Stopped gRPC WireMock server");
            }
        }
    }
    
    /**
     * gRPC ClientInterceptor that records responses to JSON files.
     * This enables the record/replay pattern for gRPC APIs.
     */
    static class RecordingInterceptor implements ClientInterceptor {
        private int callCounter = 0;
        
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel next) {
            
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    next.newCall(method, callOptions)) {
                
                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Wrap the listener to intercept responses
                    Listener<RespT> recordingListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        
                        @Override
                        public void onMessage(RespT message) {
                            // Save the response
                            saveResponse(method.getFullMethodName(), (Message) message);
                            super.onMessage(message);
                        }
                    };
                    
                    super.start(recordingListener, headers);
                }
            };
        }
        
        /**
         * Saves a gRPC response to a JSON file.
         * Method name format: google.iam.admin.v1.IAM/CreateServiceAccount
         */
        private void saveResponse(String fullMethodName, Message response) {
            try {
                // Extract method name from full path (e.g., "CreateServiceAccount" from "google.iam.admin.v1.IAM/CreateServiceAccount")
                String methodName = fullMethodName.substring(fullMethodName.lastIndexOf('/') + 1);
                
                String json = JsonFormat.printer().print(response);
                String filename = String.format("%s-%04d.json", methodName, callCounter++);
                Path filePath = Paths.get(RECORDINGS_DIR, filename);
                Files.write(filePath, json.getBytes());
                logger.info("Recorded: {}", filename);
            } catch (IOException e) {
                logger.error("Failed to record response for {}", fullMethodName, e);
            }
        }
    }
}
