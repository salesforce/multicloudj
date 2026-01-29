package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.common.service.SdkService;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.http.HttpHost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.net.URI;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Base class for substrate-specific registry implementations.
 */
public abstract class AbstractRegistry implements SdkService, Provider, AutoCloseable, OciRegistryClient.AuthProvider {
    protected final String providerId;
    protected final String repository;
    protected final String region;
    protected final String registryEndpoint;  // User-provided registry endpoint
    protected final URI proxyEndpoint;  // Proxy endpoint for HTTP client
    protected final CredentialsOverrider credentialsOverrider;
    protected OciRegistryClient ociClient;  // Created by Provider layer Builder.build() (non-final to allow initialization)
    protected final Platform targetPlatform;  // User-specified platform for multi-arch selection

    // Protected constructor - accepts Builder
    protected AbstractRegistry(Builder<?, ?> builder) {
        this.providerId = builder.getProviderId();
        this.repository = builder.getRepository();
        this.region = builder.getRegion();
        this.registryEndpoint = builder.getRegistryEndpoint();
        this.proxyEndpoint = builder.getProxyEndpoint();
        this.credentialsOverrider = builder.getCredentialsOverrider();
        this.targetPlatform = builder.getPlatform() != null ? builder.getPlatform() : Platform.DEFAULT;
        
        if (registryEndpoint == null || registryEndpoint.isEmpty()) {
            throw new IllegalStateException("Registry endpoint must be provided via builder.withRegistryEndpoint()");
        }
        
        // OciRegistryClient will be created by Provider layer Builder.build() method
    }


    @Override
    public String getProviderId() {
        return providerId;
    }

    /**
     * Returns a builder instance for this registry type.
     * Implemented by Provider layer.
     */
    public abstract Builder<?, ?> builder();

    /**
     * Pulls an image from the registry and returns an Image object.
     * Uses the platform specified in the constructor (or default linux/amd64).
     * Platform selection happens during pull, as per OCI specification.
     *
     * @param imageRef Image reference
     */
    public Image pull(String imageRef) throws IOException {
        ImageReference ref = ImageReference.parse(imageRef);
        String repo = ref.getRepository();
        String reference = ref.getReference();

        if (repo == null || repo.isEmpty()) {
            throw new IOException("Repository must be specified in imageRef");
        }

        // Use the pre-initialized OciRegistryClient
        // Repository is passed as parameter to methods, not bound to client
        try {
            OciRegistryClient.Manifest manifest = ociClient.fetchManifest(repo, reference);

            if (manifest.isIndex()) {
                // Use platform from constructor (or default)
                Platform targetPlatform = getTargetPlatform();
                String selectedDigest = selectPlatformFromIndex(manifest, targetPlatform);
                if (selectedDigest == null) {
                    throw new IOException("No matching platform found");
                }
                manifest = ociClient.fetchManifest(repo, selectedDigest);
            }

            RemoteImage remoteImage = new RemoteImage(ociClient, repo, imageRef, manifest);
            return remoteImage;
        } catch (IOException e) {
            throw e;
        }
    }


    /**
     * Extracts the image filesystem as a tar stream.
     * Similar to go-containerregistry's mutate.Extract().
     *
     * <p>This returns an InputStream containing the flattened filesystem tar stream.
     * The caller is responsible for closing the InputStream.
     *
     * @param image Image object
     * @return InputStream containing the filesystem tar stream
     * @throws IOException if extraction fails
     */
    public InputStream extractFilesystem(Image image) throws IOException {
        return doExtractFilesystem(image);
    }

    /**
     * Internal implementation for extracting filesystem as tar stream.
     * Similar to go-containerregistry's mutate.Extract().
     * 
     * <p>This method returns a stream for custom processing (e.g., parsing tar entries).
     */
    protected InputStream doExtractFilesystem(Image image) throws IOException {
        PipedInputStream pipedIn = new PipedInputStream();
        PipedOutputStream pipedOut = new PipedOutputStream(pipedIn);

        CompletableFuture.runAsync(() -> {
            try {
                flattenFilesystem(image, pipedOut);
                pipedOut.close();
            } catch (IOException e) {
                try {
                    pipedOut.close();
                } catch (IOException ignored) {}
            }
        });

        return pipedIn;
    }

    /**
     * Flattens the image filesystem by processing layers in reverse order.
     * This is the core implementation used by extract().
     * 
     * @param image Image object
     * @param outputStream OutputStream to write the flattened tar to
     * @throws IOException if flattening fails
     */
    private void flattenFilesystem(Image image, java.io.OutputStream outputStream) throws IOException {
        List<Layer> layers = image.getLayers();
        Map<String, Boolean> fileMap = new HashMap<>();

        TarArchiveOutputStream tarOut = new TarArchiveOutputStream(outputStream);
        tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX);

        try {
            for (int i = layers.size() - 1; i >= 0; i--) {
                Layer layer = layers.get(i);
                InputStream layerStream = layer.getUncompressed();
                TarArchiveInputStream tarIn = new TarArchiveInputStream(layerStream);

                TarArchiveEntry entry;
                while ((entry = tarIn.getNextTarEntry()) != null) {
                    String name = entry.getName();
                    String basename = name.substring(name.lastIndexOf('/') + 1);
                    boolean isWhiteout = basename.startsWith(".wh.");

                    if (isWhiteout) {
                        String deletedName = name.substring(0, name.length() - basename.length()) + basename.substring(4);
                        fileMap.put(deletedName, true);
                        continue;
                    }

                    if (fileMap.containsKey(name) && !fileMap.get(name)) {
                        continue;
                    }
                    if (isInWhiteoutDir(fileMap, name)) {
                        continue;
                    }

                    tarOut.putArchiveEntry(entry);
                    if (entry.getSize() > 0) {
                        IOUtils.copy(tarIn, tarOut);
                    }
                    tarOut.closeArchiveEntry();
                    fileMap.put(name, entry.isDirectory());
                }
                tarIn.close();
            }
        } finally {
            tarOut.close();
        }
    }

    private boolean isInWhiteoutDir(Map<String, Boolean> fileMap, String name) {
        String[] parts = name.split("/");
        StringBuilder path = new StringBuilder();
        for (String part : parts) {
            if (path.length() > 0) {
                path.append("/");
            }
            path.append(part);
            if (fileMap.containsKey(path.toString()) && fileMap.get(path.toString())) {
                return true;
            }
        }
        return false;
    }

    protected Platform getTargetPlatform() {
        return targetPlatform;
    }

    protected String selectPlatformFromIndex(OciRegistryClient.Manifest manifest, Platform targetPlatform) throws IOException {
        if (!manifest.isIndex()) {
            return null;
        }

        JsonArray manifests = manifest.getManifests();
        if (manifests == null || manifests.size() == 0) {
            throw new IOException("Image index contains no manifests");
        }

        for (JsonElement element : manifests) {
            JsonObject manifestDesc = element.getAsJsonObject();
            Platform manifestPlatform = parsePlatformFromDescriptor(manifestDesc);

            if (manifestPlatform.matches(targetPlatform)) {
                if (!manifestDesc.has("digest")) {
                    continue;
                }
                String digest = manifestDesc.get("digest").getAsString();
                if (digest != null && !digest.isEmpty()) {
                    return digest;
                }
            }
        }

        return null;
    }

    protected Platform parsePlatformFromDescriptor(JsonObject manifestDesc) {
        Platform.PlatformBuilder builder = Platform.builder();

        if (manifestDesc.has("platform")) {
            JsonObject platformObj = manifestDesc.getAsJsonObject("platform");
            if (platformObj != null) {
                if (platformObj.has("architecture")) {
                    builder.architecture(platformObj.get("architecture").getAsString());
                }
                if (platformObj.has("os")) {
                    builder.os(platformObj.get("os").getAsString());
                }
            }
        }

        Platform platform = builder.build();
        if (platform.getOs() == null) {
            platform = Platform.builder().os("linux").architecture("amd64").build();
        }
        return platform;
    }

    /**
     * Gets the authentication token for the registry.
     * Different providers return different types of tokens:
     * - AWS ECR: Returns token from GetAuthorizationToken API (for Basic Auth)
     * - GCP GAR: Returns OAuth2 Identity Token (for Bearer Token Exchange)
     * - ACR: Returns token from Azure Container Registry (for Bearer Token Exchange)
     */
    /**
     * Gets the authentication token.
     * This method is part of OciRegistryClient.AuthProvider interface,
     * and must be implemented by Provider layer subclasses.
     * 
     * Examples:
     * - AWS ECR: Returns token from ECR GetAuthorizationToken API
     * - GCP Artifact Registry: Returns OAuth2 Identity Token
     * - ACR: Returns token from ACR GetAuthorizationToken API
     */
    @Override
    public abstract String getAuthToken() throws IOException;
    
    /**
     * Gets the authentication username.
     * This method is part of OciRegistryClient.AuthProvider interface,
     * and must be implemented by Provider layer subclasses.
     * 
     * Examples:
     * - AWS ECR: Returns "AWS"
     * - GCP Artifact Registry: Returns "oauth2accesstoken"
     * - ACR: Returns service principal name or "00000000-0000-0000-0000-000000000000"
     */
    @Override
    public abstract String getAuthUsername() throws IOException;

    /**
     * Returns a custom HTTP client for OCI Registry operations.
     * If proxyEndpoint is configured, creates an HTTP client with proxy support.
     * Override this method in subclasses to provide additional customizations.
     * 
     * @return CloseableHttpClient instance, or null to use default
     */
    protected CloseableHttpClient getHttpClient() {
        if (proxyEndpoint != null) {
            // Create HTTP client with proxy configuration
            HttpHost proxy = new HttpHost(proxyEndpoint.getHost(), proxyEndpoint.getPort(), proxyEndpoint.getScheme());
            return HttpClients.custom()
                    .setProxy(proxy)
                    .build();
        }
        return null; // Default: use default HTTP client in OciRegistryClient
    }

    // AbstractRegistry implements OciRegistryClient.AuthProvider interface
    // The abstract methods getAuthUsername() and getAuthToken() 
    // are the interface methods, implemented by Provider layer

    public abstract Class<? extends com.salesforce.multicloudj.common.exceptions.SubstrateSdkException> getException(Throwable t);

    @Override
    public void close() throws Exception {
        // Close the OciRegistryClient created during initialization
        if (ociClient != null) {
            ociClient.close();
        }
    }

    /**
     * Abstract builder for registry implementations.
     * Provider-specific implementations should extend this and implement build().
     */
    public abstract static class Builder<A extends AbstractRegistry, T extends Builder<A, T>>
            extends RegistryBuilder<A>
            implements Provider.Builder {

        @Override
        public T providerId(String providerId) {
            super.providerId(providerId);
            return self();
        }

        /**
         * Returns self for method chaining.
         * @return this builder
         */
        public abstract T self();

        /**
         * Builds and returns an instance of AbstractRegistry.
         * Provider implementations should create OciRegistryClient in this method.
         *
         * @return An instance of AbstractRegistry.
         */
        public abstract A build();
    }
}
