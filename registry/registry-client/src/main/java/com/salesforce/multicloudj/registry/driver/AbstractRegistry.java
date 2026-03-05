package com.salesforce.multicloudj.registry.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.registry.model.Image;
import com.salesforce.multicloudj.registry.model.Layer;
import com.salesforce.multicloudj.registry.model.Manifest;
import com.salesforce.multicloudj.registry.model.Platform;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpRequestInterceptor;

/** Abstract registry driver. Each cloud implements authentication and OCI client. */
public abstract class AbstractRegistry implements Provider, AutoCloseable, AuthProvider {

  private static final String UNKNOWN = "unknown";

  protected final String providerId;
  protected final String registryEndpoint;
  protected final String region;
  protected final URI proxyEndpoint;
  protected final CredentialsOverrider credentialsOverrider;
  @Getter protected final Platform targetPlatform;

  protected AbstractRegistry(Builder<?, ?> builder) {
    this.providerId = builder.getProviderId();
    this.registryEndpoint = builder.getRegistryEndpoint();
    this.region = builder.getRegion();
    this.proxyEndpoint = builder.getProxyEndpoint();
    this.credentialsOverrider = builder.getCredentialsOverrider();
    this.targetPlatform = builder.getPlatform() != null ? builder.getPlatform() : Platform.DEFAULT;
  }

  @Override
  public String getProviderId() {
    return providerId;
  }

  /** Returns a builder instance for this registry type. */
  public abstract Builder<?, ?> builder();

  @Override
  public abstract String getAuthUsername();

  @Override
  public abstract String getAuthToken();

  /** Returns the OCI client for this registry. */
  protected abstract OciRegistryClient getOciClient();

  /**
   * Returns the list of HTTP request interceptors to be registered with the HTTP client. Override
   * this method in provider-specific subclasses to add custom interceptors. By default, returns an
   * empty list (no interceptors).
   */
  protected List<HttpRequestInterceptor> getInterceptors() {
    return Collections.emptyList();
  }

  /**
   * Pulls an image from the registry (unified OCI flow).
   *
   * <p>This method implements the OCI image pull workflow:
   *
   * <ol>
   *   <li>Parse the image reference to extract repository and reference (tag or digest)
   *   <li>Fetch the manifest from the registry
   *   <li>If the manifest is a multi-arch index, select a platform-specific manifest
   *   <li>Return a RemoteImage that lazily loads layers on demand
   * </ol>
   *
   * <p>The returned Image uses lazy loading - blobs are only downloaded when accessed via {@link
   * Image#getLayers()}.
   *
   * @param imageRef image reference (e.g. "my-repo/my-image:latest" or
   *     "my-repo/my-image@sha256:...")
   * @return Image metadata and layer descriptors (lazy-loaded)
   * @throws InvalidArgumentException if the image reference format is invalid
   * @throws UnknownException if the pull fails (network error, authentication failure, manifest not
   *     found, etc.)
   */
  public Image pull(String imageRef) {
    if (StringUtils.isBlank(imageRef)) {
      throw new InvalidArgumentException("Image reference cannot be null or empty");
    }

    // Step 1: Parse image reference
    ImageReference imageReference = ImageReference.parse(imageRef);
    String repository = imageReference.getRepository();
    String reference = imageReference.getReference();

    // Step 2: Get OCI client
    OciRegistryClient client = getOciClient();

    // Step 3: Fetch manifest
    Manifest manifest = client.fetchManifest(repository, reference);

    // Step 4: Handle multi-arch image index
    if (manifest.isIndex()) {
      String selectedDigest = selectPlatformFromIndex(manifest, targetPlatform);
      manifest = client.fetchManifest(repository, selectedDigest);
    }

    // Step 5: Create and return RemoteImage (lazy-loading)
    return new RemoteImage(client, repository, imageRef, manifest);
  }

  /**
   * Selects a platform-specific manifest from a multi-arch image index.
   *
   * <p>This method matches the target platform against each entry in the index using the
   * Platform.matches() method. The first matching entry is selected.
   *
   * <p>Platform matching follows OCI specification:
   *
   * <ul>
   *   <li>OS, Architecture, Variant, and OS version must match
   *   <li>OS features in the spec must be a subset of the entry&#39;s OS features
   *   <li>Empty/null fields in the target platform are treated as wildcards
   * </ul>
   *
   * @param indexManifest the image index manifest (must be an index)
   * @param platform the target platform to match against
   * @return the digest of the selected platform-specific manifest
   * @throws UnknownException if no matching platform is found in the index
   * @throws InvalidArgumentException if the manifest is not an index
   */
  protected String selectPlatformFromIndex(Manifest indexManifest, Platform platform) {
    if (!indexManifest.isIndex()) {
      throw new InvalidArgumentException("Manifest is not an index");
    }

    List<Manifest.IndexEntry> entries = indexManifest.getIndexManifests();
    if (entries == null || entries.isEmpty()) {
      throw new UnknownException("Image index contains no platform entries");
    }

    // Find first matching platform entry
    for (Manifest.IndexEntry entry : entries) {
      Platform entryPlatform = entry.getPlatform();
      if (entryPlatform != null && entryPlatform.matches(platform)) {
        return entry.getDigest();
      }
    }

    String targetOperatingSystem = platform.getOperatingSystem();
    String targetArchitecture = platform.getArchitecture();
    throw new UnknownException(
        String.format(
            "No manifest found for platform %s/%s in image index. Available platforms: %s",
            targetOperatingSystem != null ? targetOperatingSystem : UNKNOWN,
            targetArchitecture != null ? targetArchitecture : UNKNOWN,
            formatAvailablePlatforms(entries)));
  }

  /** Formats available platforms from index entries for error messages. */
  private String formatAvailablePlatforms(List<Manifest.IndexEntry> entries) {
    return entries.stream()
        .map(
            entry ->
                String.format(
                    "%s/%s",
                    entry.getOs() != null ? entry.getOs() : UNKNOWN,
                    entry.getArchitecture() != null ? entry.getArchitecture() : UNKNOWN))
        .collect(Collectors.joining(", "));
  }

  /**
   * Extracts the image filesystem as a tar stream.
   *
   * <p>This method implements OCI layer flattening.
   *
   * <ol>
   *   <li>Layers are processed in order (bottom to top)
   *   <li>Each layer is a tar archive that is decompressed and streamed
   *   <li>Whiteout files (.wh.*) mark deletions from lower layers
   *   <li>Opaque whiteouts (.wh..wh..opq) indicate directory replacement
   * </ol>
   *
   * <p>The returned InputStream produces a tar archive representing the flattened filesystem.
   * Caller is responsible for closing the stream.
   *
   * @param image image from a previous pull
   * @return InputStream of the flattened filesystem tar
   * @throws InvalidArgumentException if image is null or has no layers
   * @throws UnknownException if extraction fails
   */
  public InputStream extract(Image image) {
    if (image == null) {
      throw new InvalidArgumentException("Image cannot be null");
    }

    List<Layer> layers = image.getLayers();
    if (layers == null || layers.isEmpty()) {
      throw new InvalidArgumentException("Image has no layers to extract");
    }

    // Create a LayerExtractor that handles the flattening logic
    return new LayerExtractor(layers).extract();
  }

  public abstract Class<? extends SubstrateSdkException> getException(Throwable t);

  @Override
  public abstract void close() throws Exception;

  /** Abstract builder for registry implementations. */
  @Getter
  public abstract static class Builder<A extends AbstractRegistry, T extends Builder<A, T>>
      implements Provider.Builder {
    protected String providerId;
    protected String registryEndpoint;
    protected String region;
    protected URI proxyEndpoint;
    protected CredentialsOverrider credentialsOverrider;
    protected Platform platform;

    public T withRegion(String region) {
      this.region = region;
      return self();
    }

    public T withRegistryEndpoint(String registryEndpoint) {
      this.registryEndpoint = registryEndpoint;
      return self();
    }

    public T withProxyEndpoint(URI proxyEndpoint) {
      this.proxyEndpoint = proxyEndpoint;
      return self();
    }

    public T withPlatform(Platform platform) {
      this.platform = platform;
      return self();
    }

    public T withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
      this.credentialsOverrider = credentialsOverrider;
      return self();
    }

    @Override
    public T providerId(String providerId) {
      this.providerId = providerId;
      return self();
    }

    public abstract T self();

    public abstract A build();
  }
}
