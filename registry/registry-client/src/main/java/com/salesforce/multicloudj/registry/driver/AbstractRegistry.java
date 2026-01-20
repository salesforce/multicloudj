package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

import java.io.IOException;
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
     */
    public PullResult pullImage(String imageRef, Path destination) {
        if (imageRef == null || imageRef.trim().isEmpty()) {
            throw new IllegalArgumentException("Image reference cannot be null or empty");
        }
        // All providers use the same OCI Registry protocol, so we can implement it here
        return doPullImageUsingOciRegistry(imageRef, destination);
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
        OciRegistryClient.AuthProvider authProvider = () -> getDockerAuthHeader();
        
        // Create OCI Registry client
        OciRegistryClient client = new OciRegistryClient(registryEndpoint, repo, authProvider);
        
        try {
            // 1. Fetch manifest
            OciRegistryClient.Manifest manifest = client.fetchManifest(reference);
            
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
                String layerId = manifest.configDigest.replace(":", "_").replace("/", "_");
                tarBuilder.addConfig(layerId, configJson);
                
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
