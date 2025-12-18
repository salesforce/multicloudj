package com.salesforce.multicloudj.common.util.common;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
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
import lombok.Builder;
import org.wiremock.grpc.dsl.WireMockGrpcService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static org.wiremock.grpc.dsl.WireMockGrpc.json;
import static org.wiremock.grpc.dsl.WireMockGrpc.message;
import static org.wiremock.grpc.dsl.WireMockGrpc.method;

public final class WiremockGrpcUtil {

    private WiremockGrpcUtil() {
    }

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }

    /**
     * Initializes a WireMock gRPC service.
     *
     * @param port The port on which the service will listen.
     * @param serviceName The name of the service. Eg: google.iam.admin.v1.IAM
     * @param recordingsDirectory The directory where the recordings will be stored. Eg: /src/test/resources/
     * @return The initialized WireMock gRPC service.
     */
    public static WireMockGrpcService initializeWireMockGrpcService(int port, String serviceName, String recordingsDirectory) throws IOException {
        WireMockGrpcService mockGrpcService = new WireMockGrpcService(
                new WireMock(port),
                serviceName
        );
        loadGrpcStubs(recordingsDirectory, mockGrpcService);
        return mockGrpcService;
    }

    public static TransportChannelProvider getGrpcTransportChannelWithRecordingInterceptor(String recordingsDirectory) {
        RecordingInterceptor recordingInterceptor = RecordingInterceptor.builder()
                .recordingsDir(recordingsDirectory)
                .build();
        return InstantiatingGrpcChannelProvider.newBuilder()
                .setInterceptorProvider(() -> List.of(recordingInterceptor))
                .build();
    }

    /**
     * Loads all stub files from the recordings directory.
     *
     * @param stubsPath The path to the stubs directory.
     * @param mockedGrpcService The mocked gRPC service.
     * @throws IOException If an I/O error occurs.
     */
    public static void loadGrpcStubs(String stubsPath, WireMockGrpcService mockedGrpcService) throws IOException {
        Path recordingsDir = Paths.get(stubsPath);
        if (!Files.exists(recordingsDir)) {
            return;
        }

        // Load all JSON files as stubs
        try (Stream<Path> paths = Files.walk(recordingsDir)) {
            List<Path> jsonFiles = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".json"))
                    .collect(java.util.stream.Collectors.toList());

            for (Path path : jsonFiles) {
                try {
                    loadStubFromFile(path, mockedGrpcService);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to load stub from file: " + path, e);
                }
            }
        }
    }

    /**
     * Loads a single stub file and registers it with WireMock.
     *
     * @param filePath The path to the stub file.
     * @param mockedGrpcService The mocked gRPC service.
     * @throws IOException If an I/O error occurs.
     */
    private static void loadStubFromFile(Path filePath, WireMockGrpcService mockedGrpcService) throws IOException {
        // Read the response JSON
        String jsonContent = new String(Files.readAllBytes(filePath));

        JsonNode root = OBJECT_MAPPER.readTree(jsonContent);
        String methodName = root.path("methodName").asText(null);
        JsonNode requestNode = root.path("request");
        JsonNode responseNode = root.path("response");

        // Convert inner JSON nodes back to JSON strings
        String requestJson = requestNode.isMissingNode() ? null : OBJECT_MAPPER.writeValueAsString(requestNode);

        String responseJson = responseNode.isMissingNode() ? null : OBJECT_MAPPER.writeValueAsString(responseNode);

        if (methodName == null || requestJson == null || responseJson == null) {
            return;
        }
        // Register stub with WireMock
        mockedGrpcService.stubFor(
                method(methodName)
                        .withRequestMessage(equalToJson(requestJson))
                        .willReturn(json(responseJson))
        );
    }

    /**
     * Intercepts gRPC calls and records the request and response.
     *
     */
    @Builder
    static class RecordingInterceptor implements ClientInterceptor {
        private final String recordingsDir;

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method,
                CallOptions callOptions,
                Channel next) {
            // 1. Hold the request message here so the listener can access it later
            AtomicReference<ReqT> requestCapture = new AtomicReference<>();

            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    next.newCall(method, callOptions)) {

                @Override
                public void sendMessage(ReqT message) {
                    // Capture the outgoing request
                    requestCapture.set(message);
                    super.sendMessage(message);
                }

                @Override
                public void start(Listener<RespT> responseListener, Metadata headers) {
                    // Wrap the listener to intercept responses
                    Listener<RespT> recordingListener = new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {

                        @Override
                        public void onMessage(RespT message) {
                            // Save the response
                            saveRequestResponse(method.getFullMethodName(), (Message) (requestCapture.get()), (Message) message);
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
         *
         * @param fullMethodName The full method name.
         * @param request The request message.
         * @param response The response message.
         */
        private void saveRequestResponse(String fullMethodName, Message request, Message response) {
            try {
                // Extract method name from full path (e.g., "CreateServiceAccount" from "google.iam.admin.v1.IAM/CreateServiceAccount")
                String methodName = fullMethodName.substring(fullMethodName.lastIndexOf('/') + 1);

                String responseJson = JsonFormat.printer().print(response);
                String requestJson = JsonFormat.printer().print(request);

                // Parse the request/response strings into JSON trees
                JsonNode requestNode = OBJECT_MAPPER.readTree(requestJson);
                JsonNode responseNode = OBJECT_MAPPER.readTree(responseJson);

                // Build the wrapper object
                ObjectNode root = OBJECT_MAPPER.createObjectNode();
                root.put("methodName", methodName);
                root.set("request", requestNode);
                root.set("response", responseNode);

                // Serialize back to a JSON string
                String requestResponseJson = OBJECT_MAPPER.writeValueAsString(root);

                String filename = String.format("%s-%s.json", methodName, UUID.randomUUID().toString().replace("-", "").substring(0, 10));
                Path filePath = Paths.get(recordingsDir, filename);
                Files.write(filePath, requestResponseJson.getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * Saves the proto descriptor file that WireMock needs to parse gRPC requests.
     *
     * @param fileDescriptor The file descriptor to save. Eg: com.google.iam.admin.v1.ServiceAccount.getDescriptor().getFile()
     * @param grpcDir The directory where the descriptor file will be saved.
     * @param fileName The name of the descriptor file to be saved.
     */
    public static void saveProtoDescriptor(Descriptors.FileDescriptor fileDescriptor, String grpcDir, String fileName) throws IOException {
        DescriptorProtos.FileDescriptorSet.Builder descriptorSetBuilder =
                DescriptorProtos.FileDescriptorSet.newBuilder();

        Set<String> addedFiles = new HashSet<>();
        addFileDescriptorWithDependencies(fileDescriptor, descriptorSetBuilder, addedFiles);

        DescriptorProtos.FileDescriptorSet descriptorSet = descriptorSetBuilder.build();

        Path descriptorPath = Paths.get(grpcDir, fileName);
        Files.write(descriptorPath, descriptorSet.toByteArray());
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

}