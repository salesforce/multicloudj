package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.Image;
import com.salesforce.multicloudj.registry.driver.Platform;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Entry point for Client code to interact with a container registry.
 * 
 * <p>This is the Portable Layer (user-facing API) following the MultiCloudJ three-layer pattern:
 * <ul>
 *   <li>Portable layer: user-facing APIs (this class)</li>
 *   <li>Abstracted layer: OCI protocol & image semantics (AbstractRegistry, OciRegistryClient)</li>
 *   <li>Provider layer: registry-specific authentication (AwsRegistry, GcpRegistry)</li>
 * </ul>
 * 
 * <p>Similar to go-containerregistry's crane package, this provides a high-level API
 * for pulling images with platform selection support.
 * 
 * <p>Usage example:
 * <pre>
 * ContainerRegistryClient client = ContainerRegistryClient.builder("aws")
 *     .withRegistryEndpoint("https://123456789.dkr.ecr.us-east-1.amazonaws.com")
 *     .withRepository("my-repo")
 *     .withPlatform(Platform.builder().os("linux").architecture("arm64").build())
 *     .build();
 * 
 * Image img = client.pullImageAsObject("my-image:tag");
 * </pre>
 */
public class ContainerRegistryClient implements AutoCloseable {
    protected AbstractRegistry registry;

    protected ContainerRegistryClient(AbstractRegistry registry) {
        this.registry = registry;
    }

    public static ContainerRegistryClientBuilder builder(String providerId) {
        return new ContainerRegistryClientBuilder(providerId);
    }

    /**
     * Pulls a Docker image from the registry and returns an Image object.
     * Similar to go-containerregistry's crane.Pull().
     * Uses the platform specified in the builder (or default linux/amd64).
     */
    public Image pullImageAsObject(String imageRef) {
        return pullImageAsObject(imageRef, Optional.empty());
    }

    /**
     * Pulls a Docker image from the registry and returns an Image object.
     * Similar to go-containerregistry's crane.Pull().
     *
     * <p>Use this method when you discover the image is multi-arch and need to specify
     * a different platform. For example:
     * <pre>
     * // First pull: discover it's multi-arch
     * Image img1 = client.pullImageAsObject("my-image:tag");
     *
     * // Discover it's multi-arch, need linux/arm64
     * PullOptions options = PullOptions.builder()
     *     .platform(Platform.builder()
     *         .os("linux")
     *         .architecture("arm64")
     *         .build())
     *     .build();
     *
     * // Re-pull with specific platform
     * Image img2 = client.pullImageAsObject("my-image:tag", Optional.of(options));
     * </pre>
     *
     * @param imageRef Image reference (e.g., "my-image:tag")
     * @param options Optional pull options (e.g., platform selection). If empty, uses platform from builder.
     * @return Image object representing the pulled image
     */
    public Image pullImageAsObject(String imageRef, Optional<PullOptions> options) {
        try {
            Platform platform = options
                .map(PullOptions::getPlatform)
                .orElse(null);  // null means use builder's platform
            return registry.pullImageAsObject(imageRef, platform);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Lists available platforms for a multi-arch image.
     * Returns empty list if the image is not multi-arch (single platform).
     *
     * <p>Use this method to discover available platforms before pulling:
     * <pre>
     * List&lt;Platform&gt; platforms = client.listAvailablePlatforms("my-image:tag");
     * if (!platforms.isEmpty()) {
     *     // It's multi-arch, choose a platform
     *     Platform arm64 = platforms.stream()
     *         .filter(p -&gt; "arm64".equals(p.getArchitecture()))
     *         .findFirst()
     *         .orElse(null);
     *     
     *     if (arm64 != null) {
     *         PullOptions options = PullOptions.builder().platform(arm64).build();
     *         Image img = client.pullImageAsObject("my-image:tag", Optional.of(options));
     *     }
     * }
     * </pre>
     *
     * @param imageRef Image reference
     * @return List of available platforms, empty if not multi-arch
     */
    public List<Platform> listAvailablePlatforms(String imageRef) {
        try {
            return registry.listAvailablePlatforms(imageRef);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return Collections.emptyList();
        }
    }

    /**
     * Exports the image filesystem as a tarball, writing directly to the OutputStream.
     * Similar to go-containerregistry's crane.Export().
     *
     * <p>This is a convenience method that writes the filesystem tar directly to the output stream.
     * For more control, use {@link #extractFilesystem(Image)} instead.
     *
     * @param image Image object (from pullImageAsObject)
     * @param out OutputStream to write the tarball to
     * @throws SubstrateSdkException if export fails
     */
    public void exportFilesystem(Image image, OutputStream out) {
        try {
            registry.exportFilesystem(image, out);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Extracts the image filesystem as a tar stream.
     * Similar to go-containerregistry's mutate.Extract().
     *
     * <p>This returns an InputStream containing the flattened filesystem tar stream.
     * The caller is responsible for closing the InputStream.
     *
     * <p>Use this method when you need to parse the tar stream yourself (e.g., to extract
     * specific files like CSV). For simple file writing, use {@link #exportFilesystem(Image, OutputStream)}.
     *
     * @param image Image object (from pullImageAsObject)
     * @return InputStream containing the filesystem tar stream
     * @throws SubstrateSdkException if extraction fails
     */
    public InputStream extractFilesystem(Image image) {
        try {
            return registry.extractFilesystem(image);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = registry.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        if (registry != null) {
            registry.close();
        }
    }

    public static class ContainerRegistryClientBuilder {
        private final String providerId;
        private String repository;
        private String region;
        private String registryEndpoint;  // User-provided registry endpoint
        private String project;  // For GCP (optional, can be derived from endpoint)
        private String location; // For GCP (optional, can be derived from endpoint)
        private Platform platform;  // User-specified platform for multi-arch selection

        ContainerRegistryClientBuilder(String providerId) {
            this.providerId = providerId;
        }

        public ContainerRegistryClientBuilder withRepository(String repository) {
            this.repository = repository;
            return this;
        }

        public ContainerRegistryClientBuilder withRegion(String region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the registry endpoint (e.g., "https://123456789.dkr.ecr.us-east-1.amazonaws.com").
         * This is required and must be provided by the user.
         */
        public ContainerRegistryClientBuilder withRegistryEndpoint(String registryEndpoint) {
            this.registryEndpoint = registryEndpoint;
            return this;
        }

        public ContainerRegistryClientBuilder withProject(String project) {
            this.project = project;
            return this;
        }

        public ContainerRegistryClientBuilder withLocation(String location) {
            this.location = location;
            return this;
        }

        /**
         * Sets the target platform for multi-arch image selection.
         * Platform selection happens during pull, as per OCI specification.
         * If not specified, defaults to linux/amd64.
         * 
         * @param platform Platform object (e.g., Platform.builder().os("linux").architecture("arm64").build())
         * @return this builder
         */
        public ContainerRegistryClientBuilder withPlatform(Platform platform) {
            this.platform = platform;
            return this;
        }

        public ContainerRegistryClient build() {
            if (registryEndpoint == null || registryEndpoint.isEmpty()) {
                throw new IllegalArgumentException("Registry endpoint is required. Please provide via withRegistryEndpoint()");
            }

            AbstractRegistry registry;
            
            Platform targetPlatform = this.platform != null ? this.platform : Platform.DEFAULT;
            
            switch (providerId.toLowerCase()) {
                case "gcp":
                case "gar":
                    registry = new com.salesforce.multicloudj.registry.provider.GcpRegistry(
                        providerId, repository, region, registryEndpoint, null, targetPlatform);
                    break;
                case "aws":
                case "ecr":
                    registry = new com.salesforce.multicloudj.registry.provider.AwsRegistry(
                        providerId, repository, region, registryEndpoint, null, targetPlatform);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported provider: " + providerId);
            }
            
            return new ContainerRegistryClient(registry);
        }
    }
}
