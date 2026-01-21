package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Base class for substrate-specific registry implementations.
 * This class serves the purpose of providing common (i.e. substrate-agnostic) functionality.
 */
public abstract class AbstractRegistry implements SdkService, Provider, AutoCloseable {
    @Getter
    private final String providerId;
    @Getter
    protected final String repository;
    @Getter
    protected final String region;
    protected final CredentialsOverrider credentialsOverrider;

    protected AbstractRegistry(Builder<?, ?> builder) {
        this(
                builder.providerId,
                builder.repository,
                builder.region,
                builder.credentialsOverrider
        );
    }

    public AbstractRegistry(
            String providerId,
            String repository,
            String region,
            CredentialsOverrider credentials
    ) {
        this.providerId = providerId;
        this.repository = repository;
        this.region = region;
        this.credentialsOverrider = credentials;
    }

    @Override
    public String getProviderId() {
        return providerId;
    }

    /**
     * Pulls a Docker image from the registry and saves it as a tar file.
     * This method implements the standard OCI Registry (Docker Registry API v2) protocol,
     * which is the same for all providers (ECR, GAR, ACR).
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     *                 Examples: "my-image:latest", "my-image@sha256:abc123..."
     * @param destination Path where the image tar file will be saved
     * @return Result with image metadata
     * @throws IOException if image pull fails
     */
    public PullResult pullImage(String imageRef, Path destination) throws IOException {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        return doPullImageUsingOciRegistry(imageRef, destination);
    }

    /**
     * Pulls a Docker image from the registry and writes it to an OutputStream as a tar file.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     * @param outputStream OutputStream where the image tar file will be written
     * @return Result with image metadata
     * @throws IOException if image pull fails
     */
    public PullResult pullImage(String imageRef, java.io.OutputStream outputStream) throws IOException {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        return doPullImageUsingOciRegistry(imageRef, outputStream);
    }

    /**
     * Pulls a Docker image from the registry and saves it to a File as a tar file.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     * @param file File where the image tar file will be saved
     * @return Result with image metadata
     * @throws IOException if image pull fails
     */
    public PullResult pullImage(String imageRef, java.io.File file) throws IOException {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        return doPullImageUsingOciRegistry(imageRef, file.toPath());
    }

    /**
     * Pulls a Docker image from the registry and returns an InputStream for reading the tar file.
     * This provides lazy loading similar to go-containerregistry - data is streamed on-demand.
     * The caller is responsible for closing the InputStream.
     *
     * @param imageRef Image reference in the format "name:tag" or "name@digest"
     * @return Result with image metadata and InputStream for reading the tar file
     * @throws IOException if image pull fails
     */
    public PullResult pullImage(String imageRef) throws IOException {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        return doPullImageUsingOciRegistry(imageRef);
    }

    /**
     * Gets the registry endpoint URL for this provider.
     * Each provider has its own endpoint format:
     * - ECR: https://{accountId}.dkr.ecr.{region}.amazonaws.com
     * - GAR: https://{location}-docker.pkg.dev/{project}/{repository}
     * - ACR: https://{registryName}.{region}.aliyuncs.com
     *
     * @return The base URL of the Docker registry
     */
    protected abstract String getRegistryEndpoint();
    
    /**
     * Gets the target platform for image selection.
     * Default implementation returns linux/amd64 (same as go-containerregistry).
     * Subclasses can override to support custom platform selection.
     * 
     * @return Target platform (default: linux/amd64)
     */
    protected Platform getTargetPlatform() {
        return Platform.DEFAULT; // linux/amd64
    }
    
    /**
     * Selects a platform-specific manifest from an image index.
     * Similar to go-containerregistry's childByPlatform function.
     * 
     * @param manifest Image index manifest
     * @param targetPlatform Target platform to match
     * @return Digest of the selected manifest, or null if no match found
     */
    protected String selectPlatformFromIndex(OciRegistryClient.Manifest manifest, Platform targetPlatform) throws IOException {
        if (!manifest.isIndex()) {
            return null;
        }
        
        com.google.gson.JsonArray manifests = manifest.getManifests();
        if (manifests == null || manifests.size() == 0) {
            throw new IOException("Image index contains no manifests");
        }
        
        // Iterate through manifests to find matching platform
        for (com.google.gson.JsonElement element : manifests) {
            com.google.gson.JsonObject manifestDesc = element.getAsJsonObject();
            
            // Extract platform from descriptor
            Platform manifestPlatform = parsePlatformFromDescriptor(manifestDesc);
            
            // Check if platform matches
            if (manifestPlatform.matches(targetPlatform)) {
                // Extract digest (digest is a string field in the descriptor)
                if (!manifestDesc.has("digest")) {
                    continue;
                }
                String digest = manifestDesc.get("digest").getAsString();
                if (digest == null || digest.isEmpty()) {
                    continue;
                }
                return digest;
            }
        }
        
        // No matching platform found
        return null;
    }
    
    /**
     * Parses platform information from a manifest descriptor.
     * Similar to go-containerregistry's logic: if platform is missing, assume amd64/linux.
     * 
     * @param manifestDesc Manifest descriptor JSON object
     * @return Platform object (defaults to linux/amd64 if not specified)
     */
    protected Platform parsePlatformFromDescriptor(com.google.gson.JsonObject manifestDesc) {
        Platform.PlatformBuilder builder = Platform.builder();
        
        // Extract platform from descriptor
        if (manifestDesc.has("platform")) {
            com.google.gson.JsonObject platformObj = manifestDesc.getAsJsonObject("platform");
            if (platformObj != null) {
                if (platformObj.has("architecture")) {
                    builder.architecture(platformObj.get("architecture").getAsString());
                }
                if (platformObj.has("os")) {
                    builder.os(platformObj.get("os").getAsString());
                }
                if (platformObj.has("os.version")) {
                    builder.osVersion(platformObj.get("os.version").getAsString());
                }
                if (platformObj.has("variant")) {
                    builder.variant(platformObj.get("variant").getAsString());
                }
                if (platformObj.has("os.features")) {
                    com.google.gson.JsonArray osFeatures = platformObj.getAsJsonArray("os.features");
                    if (osFeatures != null) {
                        java.util.List<String> features = new java.util.ArrayList<>();
                        for (com.google.gson.JsonElement feature : osFeatures) {
                            features.add(feature.getAsString());
                        }
                        builder.osFeatures(features);
                    }
                }
                if (platformObj.has("features")) {
                    com.google.gson.JsonArray features = platformObj.getAsJsonArray("features");
                    if (features != null) {
                        java.util.List<String> featureList = new java.util.ArrayList<>();
                        for (com.google.gson.JsonElement feature : features) {
                            featureList.add(feature.getAsString());
                        }
                        builder.features(featureList);
                    }
                }
            }
        }
        
        // If platform is missing from descriptor, assume amd64/linux (same as go-containerregistry)
        Platform platform = builder.build();
        if (platform.getOs() == null || platform.getOs().isEmpty()) {
            return Platform.DEFAULT; // linux/amd64
        }
        if (platform.getArchitecture() == null || platform.getArchitecture().isEmpty()) {
            // If OS is specified but architecture is missing, assume amd64
            return Platform.builder()
                    .os(platform.getOs())
                    .architecture("amd64")
                    .osVersion(platform.getOsVersion())
                    .variant(platform.getVariant())
                    .osFeatures(platform.getOsFeatures())
                    .features(platform.getFeatures())
                    .build();
        }
        
        return platform;
    }

    /**
     * Common implementation for pulling image using standard OCI Registry API v2.
     * All providers (ECR, GAR, ACR) use the same protocol.
     * 
     * Reference: go-containerregistry's remote.Image implementation
     * 
     * Flow:
     * 1. Parse image reference (name:tag or name@digest)
     * 2. GET /v2/{repository}/manifests/{reference} - Get manifest (with auth challenge handling)
     * 3. Parse manifest to get layer digests and config
     * 4. GET /v2/{repository}/blobs/{digest} - Download each layer (streaming)
     * 5. Combine layers into tar file
     */
    protected PullResult doPullImageUsingOciRegistry(String imageRef, Path destination) throws IOException {
        // Parse image reference
        ImageReference ref = ImageReference.parse(imageRef);
        
        // Use the repository from builder, or from imageRef if specified
        String repo = this.repository != null ? this.repository : ref.getRepository();
        String reference = ref.getReference(); // tag or digest
        
        // Get registry endpoint and auth
        String registryEndpoint = getRegistryEndpoint();
        OciRegistryClient.AuthProvider authProvider = new OciRegistryClient.AuthProvider() {
            @Override
            public String getBasicAuthHeader() throws IOException {
                return AbstractRegistry.this.getDockerAuthHeader();
            }

            @Override
            public String getIdentityToken() throws IOException {
                // For GCP, this returns OAuth2 access token
                // For ECR/ACR, this returns null (not needed)
                return AbstractRegistry.this.getIdentityToken();
            }
        };
        
        // Create OCI Registry client
        OciRegistryClient client = new OciRegistryClient(registryEndpoint, repo, authProvider);
        
        try {
            // 1. Fetch manifest
            OciRegistryClient.Manifest manifest = client.fetchManifest(reference);
            
            // 1.1. Handle multi-arch image index
            if (manifest.isIndex()) {
                // Select platform based on default (linux/amd64) or user-specified platform
                // Similar to go-containerregistry's childByPlatform logic
                Platform targetPlatform = getTargetPlatform(); // Default: linux/amd64
                String selectedDigest = selectPlatformFromIndex(manifest, targetPlatform);
                if (selectedDigest == null) {
                    throw new IOException(String.format(
                        "No matching platform found in image index. Required: %s/%s",
                        targetPlatform.getOs(), targetPlatform.getArchitecture()));
                }
                // Fetch the specific manifest for the selected platform
                manifest = client.fetchManifest(selectedDigest);
            }
            
            // 2. Download config
            String configJson;
            try (InputStream configStream = client.downloadBlob(manifest.configDigest)) {
                java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
                byte[] data = new byte[8192];
                int nRead;
                while ((nRead = configStream.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
                configJson = buffer.toString(StandardCharsets.UTF_8.name());
            }
            
            // 3. Build Docker tar file
            DockerTarBuilder tarBuilder = new DockerTarBuilder(destination);
            try {
                // Add VERSION file (legacy format)
                tarBuilder.addVersion();
                
                // Add config.json
                String configId = manifest.configDigest.replace(":", "_").replace("/", "_");
                tarBuilder.addConfig(configId, configJson);
                
                // Download and add each layer
                for (String layerDigest : manifest.layerDigests) {
                    String layerId = layerDigest.replace(":", "_").replace("/", "_");
                    try (InputStream layerStream = client.downloadBlob(layerDigest)) {
                        tarBuilder.addLayer(layerId, layerStream);
                    }
                }
                
                // Add manifest.json (Docker format)
                // Format: [{"Config":"<config-digest>.json","RepoTags":["<repo>:<tag>"],"Layers":["<layer-digest>/layer.tar",...]}]
                String manifestJson = buildManifestJson(repo, reference, manifest);
                tarBuilder.addManifest(manifestJson);
                
            } finally {
                tarBuilder.close();
            }
            
            // 4. Build result
            ImageMetadata metadata = ImageMetadata.builder()
                .digest(manifest.configDigest)
                .tag(ref.isDigest() ? null : reference)
                .build();
            
            return PullResult.builder()
                .metadata(metadata)
                .savedPath(destination.toString())
                .build();
                
        } finally {
            client.close();
        }
    }

    /**
     * Common implementation for pulling image to OutputStream.
     */
    protected PullResult doPullImageUsingOciRegistry(String imageRef, java.io.OutputStream outputStream) throws IOException {
        // Parse image reference
        ImageReference ref = ImageReference.parse(imageRef);
        
        // Use the repository from builder, or from imageRef if specified
        String repo = this.repository != null ? this.repository : ref.getRepository();
        String reference = ref.getReference(); // tag or digest
        
        // Get registry endpoint and auth
        String registryEndpoint = getRegistryEndpoint();
        OciRegistryClient.AuthProvider authProvider = new OciRegistryClient.AuthProvider() {
            @Override
            public String getBasicAuthHeader() throws IOException {
                return AbstractRegistry.this.getDockerAuthHeader();
            }

            @Override
            public String getIdentityToken() throws IOException {
                return AbstractRegistry.this.getIdentityToken();
            }
        };
        
        // Create OCI Registry client
        OciRegistryClient client = new OciRegistryClient(registryEndpoint, repo, authProvider);
        
        try {
            // 1. Fetch manifest
            OciRegistryClient.Manifest manifest = client.fetchManifest(reference);
            
            // 1.1. Handle multi-arch image index
            if (manifest.isIndex()) {
                // Select platform based on default (linux/amd64) or user-specified platform
                // Similar to go-containerregistry's childByPlatform logic
                Platform targetPlatform = getTargetPlatform(); // Default: linux/amd64
                String selectedDigest = selectPlatformFromIndex(manifest, targetPlatform);
                if (selectedDigest == null) {
                    throw new IOException(String.format(
                        "No matching platform found in image index. Required: %s/%s",
                        targetPlatform.getOs(), targetPlatform.getArchitecture()));
                }
                // Fetch the specific manifest for the selected platform
                manifest = client.fetchManifest(selectedDigest);
            }
            
            // 2. Download config
            String configJson;
            try (InputStream configStream = client.downloadBlob(manifest.configDigest)) {
                java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
                byte[] data = new byte[8192];
                int nRead;
                while ((nRead = configStream.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, nRead);
                }
                configJson = buffer.toString(StandardCharsets.UTF_8.name());
            }
            
            // 3. Build Docker tar file to OutputStream
            DockerTarBuilder tarBuilder = new DockerTarBuilder(outputStream, false);
            try {
                // Add VERSION file (legacy format)
                tarBuilder.addVersion();
                
                // Add config.json
                String configId = manifest.configDigest.replace(":", "_").replace("/", "_");
                tarBuilder.addConfig(configId, configJson);
                
                // Download and add each layer
                for (String layerDigest : manifest.layerDigests) {
                    String layerId = layerDigest.replace(":", "_").replace("/", "_");
                    try (InputStream layerStream = client.downloadBlob(layerDigest)) {
                        tarBuilder.addLayer(layerId, layerStream);
                    }
                }
                
                // Add manifest.json (Docker format)
                String manifestJson = buildManifestJson(repo, reference, manifest);
                tarBuilder.addManifest(manifestJson);
                
            } finally {
                tarBuilder.close();
            }
            
            // 4. Build result
            ImageMetadata metadata = ImageMetadata.builder()
                .digest(manifest.configDigest)
                .tag(ref.isDigest() ? null : reference)
                .build();
            
            return PullResult.builder()
                .metadata(metadata)
                .savedPath(null)  // Not saved to file
                .build();
                
        } finally {
            client.close();
        }
    }

    /**
     * Common implementation for pulling image and returning InputStream (lazy loading).
     * Uses PipedInputStream/PipedOutputStream to stream the tar file on-demand.
     * The actual download happens in a background thread as the InputStream is read.
     */
    protected PullResult doPullImageUsingOciRegistry(String imageRef) throws IOException {
        // Parse image reference
        ImageReference ref = ImageReference.parse(imageRef);
        
        // Use the repository from builder, or from imageRef if specified
        String repo = this.repository != null ? this.repository : ref.getRepository();
        String reference = ref.getReference(); // tag or digest
        
        // Get registry endpoint and auth
        String registryEndpoint = getRegistryEndpoint();
        OciRegistryClient.AuthProvider authProvider = new OciRegistryClient.AuthProvider() {
            @Override
            public String getBasicAuthHeader() throws IOException {
                return AbstractRegistry.this.getDockerAuthHeader();
            }

            @Override
            public String getIdentityToken() throws IOException {
                return AbstractRegistry.this.getIdentityToken();
            }
        };
        
        // Create PipedInputStream/PipedOutputStream for streaming
        // Buffer size: 1MB for better performance
        java.io.PipedInputStream pipedInputStream = new java.io.PipedInputStream(1024 * 1024);
        java.io.PipedOutputStream pipedOutputStream = new java.io.PipedOutputStream(pipedInputStream);
        
        // Store reference for the background thread
        final String finalRepo = repo;
        final String finalReference = reference;
        final String finalRegistryEndpoint = registryEndpoint;
        
        // Start background thread to build tar and write to PipedOutputStream
        // This allows lazy loading - data is streamed on-demand as InputStream is read
        Thread writerThread = new Thread(() -> {
            try {
                // Create OCI Registry client
                OciRegistryClient client = new OciRegistryClient(finalRegistryEndpoint, finalRepo, authProvider);
                
                try {
                    // 1. Fetch manifest
                    OciRegistryClient.Manifest manifest = client.fetchManifest(finalReference);
                    
                    // 1.1. Handle multi-arch image index
                    if (manifest.isIndex()) {
                        // Select platform based on default (linux/amd64) or user-specified platform
                        // Similar to go-containerregistry's childByPlatform logic
                        Platform targetPlatform = getTargetPlatform(); // Default: linux/amd64
                        String selectedDigest = selectPlatformFromIndex(manifest, targetPlatform);
                        if (selectedDigest == null) {
                            throw new IOException(String.format(
                                "No matching platform found in image index. Required: %s/%s",
                                targetPlatform.getOs(), targetPlatform.getArchitecture()));
                        }
                        // Fetch the specific manifest for the selected platform
                        manifest = client.fetchManifest(selectedDigest);
                    }
                    
                    // 2. Download config
                    String configJson;
                    try (InputStream configStream = client.downloadBlob(manifest.configDigest)) {
                        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
                        byte[] data = new byte[8192];
                        int nRead;
                        while ((nRead = configStream.read(data, 0, data.length)) != -1) {
                            buffer.write(data, 0, nRead);
                        }
                        configJson = buffer.toString(StandardCharsets.UTF_8.name());
                    }
                    
                    // 3. Build Docker tar file to PipedOutputStream
                    DockerTarBuilder tarBuilder = new DockerTarBuilder(pipedOutputStream, false);
                    try {
                        // Add VERSION file (legacy format)
                        tarBuilder.addVersion();
                        
                        // Add config.json
                        String configId = manifest.configDigest.replace(":", "_").replace("/", "_");
                        tarBuilder.addConfig(configId, configJson);
                        
                        // Download and add each layer
                        for (String layerDigest : manifest.layerDigests) {
                            String layerId = layerDigest.replace(":", "_").replace("/", "_");
                            try (InputStream layerStream = client.downloadBlob(layerDigest)) {
                                tarBuilder.addLayer(layerId, layerStream);
                            }
                        }
                        
                        // Add manifest.json (Docker format)
                        String manifestJson = buildManifestJson(finalRepo, finalReference, manifest);
                        tarBuilder.addManifest(manifestJson);
                        
                    } finally {
                        tarBuilder.close();
                        pipedOutputStream.close();  // Close pipe to signal EOF
                    }
                        
                } finally {
                    client.close();
                }
            } catch (IOException e) {
                try {
                    pipedOutputStream.close();
                } catch (IOException ignored) {}
                // Note: IOException will be thrown when reading from pipedInputStream
            }
        }, "ImagePull-StreamWriter");
        writerThread.setDaemon(true);
        writerThread.start();
        
        // Return result immediately with InputStream (lazy loading)
        // The actual download happens in the background thread as InputStream is read
        ImageMetadata metadata = ImageMetadata.builder()
            .digest("streaming")  // Will be available after stream is consumed
            .tag(ref.isDigest() ? null : reference)
            .build();
        
        return PullResult.builder()
            .metadata(metadata)
            .savedPath(null)
            .inputStream(pipedInputStream)
            .build();
    }
    
    /**
     * Builds the manifest.json content for Docker image tar format.
     */
    private String buildManifestJson(String repo, String reference, OciRegistryClient.Manifest manifest) {
        StringBuilder json = new StringBuilder("[{\"Config\":\"");
        String configId = manifest.configDigest.replace(":", "_").replace("/", "_");
        json.append(configId).append(".json");
        json.append("\",\"RepoTags\":[\"").append(repo).append(":").append(reference).append("\"],\"Layers\":[");
        
        for (int i = 0; i < manifest.layerDigests.size(); i++) {
            if (i > 0) json.append(",");
            String layerId = manifest.layerDigests.get(i).replace(":", "_").replace("/", "_");
            json.append("\"").append(layerId).append("/layer.tar\"");
        }
        
        json.append("]}]");
        return json.toString();
    }


    /**
     * Gets the Docker Registry authentication token for this provider.
     * Each provider implements its own token retrieval logic:
     * - ECR: Uses GetAuthorizationToken API (calls AWS ECR API)
     * - GAR: Uses OAuth2 access token (from GoogleCredentials)
     * - ACR: Uses Aliyun token (calls Aliyun API)
     *
     * @return The authentication token as a string
     * @throws IOException if token retrieval fails
     */
    protected abstract String getDockerAuthToken() throws IOException;

    /**
     * Gets the Docker Registry authentication username for this provider.
     * Each provider has its own username format:
     * - ECR: "AWS"
     * - GAR: "oauth2accesstoken"
     * - ACR: Aliyun account name
     *
     * @return The username for Docker Registry Basic Auth
     */
    protected abstract String getDockerAuthUsername();

    /**
     * Gets the Identity Token (OAuth2 access token) for Bearer Token exchange.
     * This is used for registries that require Bearer Token authentication (e.g., GCP Artifact Registry).
     * 
     * For providers that don't need Bearer Token exchange (e.g., ECR, ACR), 
     * this should return null.
     *
     * @return OAuth2 access token, or null if not applicable
     * @throws IOException if token retrieval fails
     */
    protected String getIdentityToken() throws IOException {
        // Default implementation: return null (for ECR/ACR)
        // GCP implementations should override this to return OAuth2 access token
        return null;
    }

    /**
     * Constructs the Docker Registry HTTP Basic Authentication header.
     * This is a common implementation that uses provider-specific token and username.
     * Format: "Basic base64(username:token)"
     *
     * @return The Authorization header value (e.g., "Basic dXNlcm5hbWU6dG9rZW4=")
     * @throws IOException if token retrieval fails
     */
    protected final String getDockerAuthHeader() throws IOException {
        String username = getDockerAuthUsername();
        String token = getDockerAuthToken();
        String credentials = username + ":" + token;
        String encoded = java.util.Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    /**
     * Maps provider-specific exceptions to MultiCloudJ common exceptions.
     * Must be implemented by each provider.
     *
     * @param t The throwable to map
     * @return The corresponding SubstrateSdkException class
     */
    public abstract Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> getException(Throwable t);

    @Override
    public void close() throws Exception {
        // Default no-op close
    }

    public abstract static class Builder<T extends AbstractRegistry, B extends Builder<T, B>>
            implements Provider.Builder {
        @Getter
        protected String providerId;
        @Getter
        protected String repository;
        @Getter
        protected String region;
        @Getter
        protected URI endpoint;
        @Getter
        protected URI proxyEndpoint;
        @Getter
        protected CredentialsOverrider credentialsOverrider;
        @Getter
        protected Properties properties = new Properties();
        @Getter
        protected RetryConfig retryConfig;

        @Override
        public B providerId(String providerId) {
            this.providerId = providerId;
            return self();
        }

        public B withRepository(String repository) {
            this.repository = repository;
            return self();
        }

        public B withRegion(String region) {
            this.region = region;
            return self();
        }

        public B withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return self();
        }

        public B withProxyEndpoint(URI proxyEndpoint) {
            this.proxyEndpoint = proxyEndpoint;
            return self();
        }

        public B withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.credentialsOverrider = credentialsOverrider;
            return self();
        }

        public B withProperties(Properties properties) {
            this.properties = properties;
            return self();
        }

        public B withRetryConfig(RetryConfig retryConfig) {
            this.retryConfig = retryConfig;
            return self();
        }

        public abstract B self();

        public abstract T build();
    }
}
