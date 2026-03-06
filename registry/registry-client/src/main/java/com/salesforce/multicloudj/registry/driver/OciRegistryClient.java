package com.salesforce.multicloudj.registry.driver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.registry.model.Manifest;
import com.salesforce.multicloudj.registry.model.Platform;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

/** OCI Registry API v2 HTTP client; handles auth and registry operations. */
public class OciRegistryClient implements AutoCloseable {

    private static final String AUTH_SCHEME_ANONYMOUS = "anonymous";
    private static final String AUTH_SCHEME_BASIC = "basic";
    private static final String AUTH_SCHEME_BEARER = "bearer";
    private static final String MANIFEST_ACCEPT_HEADER =
            OciManifestFields.MEDIA_TYPE_OCI_MANIFEST + ","
                    + OciManifestFields.MEDIA_TYPE_OCI_INDEX + ","
                    + OciManifestFields.MEDIA_TYPE_DOCKER_MANIFEST + ","
                    + OciManifestFields.MEDIA_TYPE_DOCKER_MANIFEST_LIST;
    private static final String DIGEST_ALGORITHM = "SHA-256";
    private static final String DIGEST_PREFIX = "sha256:";
    private static final int MAX_MANIFEST_SIZE_BYTES = 100 * 1024 * 1024; // 100 MB


    private final String registryEndpoint;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;
    private final BearerTokenExchange tokenExchange;

    /** Lock for thread-safe lazy initialization of cachedChallenge. */
    private final Object challengeLock = new Object();

    /** Cached auth challenge, lazily initialized with double-checked locking. */
    private volatile AuthChallenge cachedChallenge;

    /**
     * Creates a new OciRegistryClient.
     *
     * @param registryEndpoint the registry base URL
     * @param registry the registry
     */
    public OciRegistryClient(String registryEndpoint, AbstractRegistry registry) {
        this(registryEndpoint, registry, null);
    }

    /**
     * Creates OciRegistryClient with specified HttpClient.
     *
     * @param registryEndpoint the registry base URL
     * @param registry the registry
     * @param httpClient the HTTP client to use (null to create default)
     */
    public OciRegistryClient(String registryEndpoint, AbstractRegistry registry,
                            CloseableHttpClient httpClient) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = registry;
        if (httpClient != null) {
            this.httpClient = httpClient;
        } else {
            HttpClientBuilder builder = HttpClients.custom();
            for (HttpRequestInterceptor interceptor : registry.getInterceptors()) {
                builder.addInterceptorLast(interceptor);
            }
            this.httpClient = builder.build();
        }
        this.tokenExchange = new BearerTokenExchange(this.httpClient);
    }

  /**
   * Builds HTTP Authorization header for the given repository. Discovers auth requirements on first
   * call and caches the result.
   *
   * @param repository the repository name (used for Bearer token scope)
   * @return the Authorization header value, or null if anonymous access (no auth required)
   * @throws InvalidArgumentException if authentication scheme is blank or missing
   * @throws UnSupportedOperationException if authentication scheme is not supported
   */
  public String getHttpAuthHeader(String repository) {
    // Double-checked locking for thread-safe lazy initialization
    if (cachedChallenge == null) {
      synchronized (challengeLock) {
        if (cachedChallenge == null) {
          cachedChallenge = AuthChallenge.discover(httpClient, registryEndpoint);
        }
      }
    }

    final String scheme = cachedChallenge.getScheme();
    if (StringUtils.isBlank(scheme)) {
      throw new InvalidArgumentException("Authentication scheme is blank or missing");
    }

    switch (scheme.toLowerCase()) {
      case AUTH_SCHEME_ANONYMOUS:
        return null;
      case AUTH_SCHEME_BASIC:
        return buildBasicAuthHeader();
      case AUTH_SCHEME_BEARER:
        return buildBearerAuthHeader(repository);
      default:
        throw new UnSupportedOperationException("Unsupported authentication scheme: " + scheme);
    }
  }

  private String buildBasicAuthHeader() {
    String username = authProvider.getAuthUsername();
    String token = authProvider.getAuthToken();
    String credentials = username + ":" + token;
    String encoded =
        Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    return "Basic " + encoded;
  }

  private String buildBearerAuthHeader(String repository) {
    // Get identity token from provider (e.g., GCP OAuth2 access token)
    String identityToken = authProvider.getAuthToken();

    // Exchange for registry-scoped bearer token
    String bearerToken =
        tokenExchange.getBearerToken(cachedChallenge, identityToken, repository, "pull");

    return "Bearer " + bearerToken;
  }

  /**
   * Fetches the image manifest for the given repository and reference (tag or digest). HTTP GET
   * /v2/{repository}/manifests/{reference}.
   *
   * @param repository the repository name (e.g., "my-repo/my-image")
   * @param reference the tag or digest (e.g., "latest" or "sha256:...")
   * @return the parsed Manifest object
   * @throws ResourceNotFoundException if manifest not found (404)
   * @throws UnAuthorizedException if authentication fails (401)
   * @throws UnknownException if the request fails or manifest cannot be parsed
   */
  public Manifest fetchManifest(String repository, String reference) {
    String url = String.format("%s/v2/%s/manifests/%s", registryEndpoint, repository, reference);

    HttpGet request = new HttpGet(url);
    request.setHeader(HttpHeaders.ACCEPT, MANIFEST_ACCEPT_HEADER);
    String authHeader = getHttpAuthHeader(repository);
    if (authHeader != null) {
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    try {
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != HttpStatus.SC_OK) {
          String errorBody =
              response.getEntity() != null
                  ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)
                  : StringUtils.EMPTY;
          if (statusCode == HttpStatus.SC_NOT_FOUND) {
            throw new ResourceNotFoundException(
                String.format(
                    "Failed to fetch manifest for %s:%s from %s - HTTP %d: %s",
                    repository, reference, registryEndpoint, statusCode, errorBody));
          }
          if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
            throw new UnAuthorizedException(
                String.format(
                    "Failed to fetch manifest for %s:%s from %s - HTTP %d: %s",
                    repository, reference, registryEndpoint, statusCode, errorBody));
          }
          throw new UnknownException(
              String.format(
                  "Failed to fetch manifest for %s:%s from %s - HTTP %d: %s",
                  repository, reference, registryEndpoint, statusCode, errorBody));
        }

        if (response.getEntity() == null) {
          throw new UnknownException("Failed to fetch manifest: empty response body");
        }

        // Check manifest size limit (100 MB)
        long contentLength = response.getEntity().getContentLength();
        if (contentLength > MAX_MANIFEST_SIZE_BYTES) {
          throw new UnknownException(
              String.format(
                  "Manifest size (%d bytes) exceeds maximum allowed size (%d bytes)",
                  contentLength, MAX_MANIFEST_SIZE_BYTES));
        }

        String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        Header header = response.getFirstHeader("Docker-Content-Digest");
        String digestHeader = header != null ? header.getValue() : null;

        // If Docker-Content-Digest header is missing (e.g., AWS ECR), calculate it from the
        // response body
        if (digestHeader == null) {
          try {
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
            byte[] hash = md.digest(responseBody.getBytes(StandardCharsets.UTF_8));
            digestHeader = DIGEST_PREFIX + Hex.encodeHexString(hash);
          } catch (NoSuchAlgorithmException e) {
            throw new UnknownException(
                "Failed to calculate manifest digest: "
                    + DIGEST_ALGORITHM
                    + " SHA-256 algorithm not available",
                e);
          }
        }

        // Validate digest if fetching by digest
        if (reference.startsWith(DIGEST_PREFIX)) {
          if (!reference.equals(digestHeader)) {
            throw new UnknownException(
                String.format(
                    "Manifest digest mismatch: expected %s, got %s for %s:%s",
                    reference, digestHeader, repository, reference));
          }
        }

        return parseManifestResponse(responseBody, digestHeader);
      }
    } catch (IOException e) {
      throw new UnknownException("Failed to fetch manifest", e);
    }
  }

  /**
   * Parses the manifest JSON response into a Manifest object. Handles both image manifests and
   * image indexes (multi-arch).
   */
  private Manifest parseManifestResponse(String responseBody, String digest) {
    JsonObject json;
    try {
      json = JsonParser.parseString(responseBody).getAsJsonObject();
    } catch (JsonSyntaxException e) {
      throw new UnknownException(
          "Invalid JSON response from registry: "
              + responseBody.substring(0, Math.min(200, responseBody.length())),
          e);
    }

    String mediaType =
        json.has(OciManifestFields.MEDIA_TYPE)
            ? json.get(OciManifestFields.MEDIA_TYPE).getAsString()
            : "";

    // Check if this is an image index (multi-arch)
    if (OciManifestFields.MEDIA_TYPE_OCI_INDEX.equals(mediaType)
        || OciManifestFields.MEDIA_TYPE_DOCKER_MANIFEST_LIST.equals(mediaType)
        || json.has(OciManifestFields.MANIFESTS)) {
      return parseImageIndex(json, digest);
    }

    // Single image manifest
    return parseImageManifest(json, digest);
  }

  /** Parses annotations from a manifest or index JSON. */
  private Map<String, String> parseAnnotations(JsonObject json) {
    if (!json.has(OciManifestFields.ANNOTATIONS)) {
      return null;
    }
    JsonObject annotationsObj = json.getAsJsonObject(OciManifestFields.ANNOTATIONS);
    Map<String, String> annotations = new HashMap<>(annotationsObj.size());
    for (Map.Entry<String, JsonElement> entry : annotationsObj.entrySet()) {
      annotations.put(entry.getKey(), entry.getValue().getAsString());
    }
    return annotations;
  }

  /** Parses subject field from a manifest or index JSON. */
  private String parseSubject(JsonObject json) {
    if (!json.has(OciManifestFields.SUBJECT)) {
      return null;
    }
    JsonObject subjectObj = json.getAsJsonObject(OciManifestFields.SUBJECT);
    return subjectObj.has(OciManifestFields.DIGEST)
        ? subjectObj.get(OciManifestFields.DIGEST).getAsString()
        : null;
  }

  /**
   * Parses platform information from a manifest descriptor in an image index. Returns null if no
   * platform field exists.
   *
   * @param manifestDesc the manifest descriptor JSON object
   * @return Platform object or null if no platform information
   */
  private Platform parsePlatform(JsonObject manifestDesc) {
    if (!manifestDesc.has(OciManifestFields.PLATFORM)) {
      return null;
    }

    JsonObject platformObj = manifestDesc.getAsJsonObject(OciManifestFields.PLATFORM);
    String operatingSystem =
        platformObj.has(OciManifestFields.OS)
            ? platformObj.get(OciManifestFields.OS).getAsString()
            : null;
    String architecture =
        platformObj.has(OciManifestFields.ARCHITECTURE)
            ? platformObj.get(OciManifestFields.ARCHITECTURE).getAsString()
            : null;
    String osVersion =
        platformObj.has(OciManifestFields.OS_VERSION)
            ? platformObj.get(OciManifestFields.OS_VERSION).getAsString()
            : null;
    String variant =
        platformObj.has(OciManifestFields.VARIANT)
            ? platformObj.get(OciManifestFields.VARIANT).getAsString()
            : null;

    List<String> osFeatures = parseOsFeatures(platformObj);

    return Platform.builder()
        .operatingSystem(operatingSystem)
        .architecture(architecture)
        .operatingSystemVersion(osVersion)
        .variant(variant)
        .operatingSystemFeatures(osFeatures)
        .build();
  }

  /**
   * Parses os.features array from a platform object. Returns null if no os.features field exists.
   *
   * @param platformObj the platform JSON object
   * @return List of OS feature strings or null
   */
  private List<String> parseOsFeatures(JsonObject platformObj) {
    if (!platformObj.has(OciManifestFields.OS_FEATURES)) {
      return null;
    }

    JsonArray osFeaturesArray = platformObj.getAsJsonArray(OciManifestFields.OS_FEATURES);
    if (osFeaturesArray == null) {
      return null;
    }

    List<String> osFeatures = new ArrayList<>(osFeaturesArray.size());
    for (JsonElement featureElement : osFeaturesArray) {
      osFeatures.add(featureElement.getAsString());
    }
    return osFeatures;
  }

  /** Parses an image index (multi-architecture manifest list). */
  private Manifest parseImageIndex(JsonObject json, String digest) {
    JsonArray manifestsArray = json.getAsJsonArray(OciManifestFields.MANIFESTS);
    int initialCapacity = manifestsArray != null ? manifestsArray.size() : 0;
    List<Manifest.IndexEntry> entries = new ArrayList<>(initialCapacity);

    if (manifestsArray == null) {
      return Manifest.index(entries, digest);
    }

    for (JsonElement element : manifestsArray) {
      JsonObject manifestDesc = element.getAsJsonObject();
      if (!manifestDesc.has(OciManifestFields.DIGEST)) {
        throw new InvalidArgumentException(
            "Invalid image index: manifest entry missing required 'digest' field");
      }
      String manifestDigest = manifestDesc.get(OciManifestFields.DIGEST).getAsString();
      Platform platform = parsePlatform(manifestDesc);
      entries.add(new Manifest.IndexEntry(manifestDigest, platform));
    }

    return Manifest.index(entries, digest, parseAnnotations(json), parseSubject(json));
  }

  /** Parses a single image manifest. */
  private Manifest parseImageManifest(JsonObject json, String digest) {
    // Get config digest
    String configDigest = null;
    if (json.has(OciManifestFields.CONFIG)) {
      JsonObject config = json.getAsJsonObject(OciManifestFields.CONFIG);
      if (config != null && config.has(OciManifestFields.DIGEST)) {
        configDigest = config.get(OciManifestFields.DIGEST).getAsString();
      }
    }

    List<Manifest.LayerInfo> layerInfos = parseLayerInfos(json);
    return Manifest.image(
        configDigest, layerInfos, digest, parseAnnotations(json), parseSubject(json));
  }

  /** Parses layer information from a manifest JSON object. */
  private List<Manifest.LayerInfo> parseLayerInfos(JsonObject json) {
    JsonArray layers =
        json.has(OciManifestFields.LAYERS) ? json.getAsJsonArray(OciManifestFields.LAYERS) : null;
    int layerCapacity = layers != null ? layers.size() : 0;
    List<Manifest.LayerInfo> layerInfos = new ArrayList<>(layerCapacity);
    if (layers != null) {
      for (JsonElement element : layers) {
        JsonObject layer = element.getAsJsonObject();
        if (!layer.has(OciManifestFields.DIGEST)) {
          throw new InvalidArgumentException(
              "Invalid image manifest: layer missing required 'digest' field");
        }
        String layerDigest = layer.get(OciManifestFields.DIGEST).getAsString();
        String mediaType =
            layer.has(OciManifestFields.MEDIA_TYPE)
                ? layer.get(OciManifestFields.MEDIA_TYPE).getAsString()
                : null;
        Long size =
            layer.has(OciManifestFields.SIZE)
                ? layer.get(OciManifestFields.SIZE).getAsLong()
                : null;
        layerInfos.add(new Manifest.LayerInfo(layerDigest, mediaType, size));
      }
    }
    return layerInfos;
  }

  /**
   * Downloads a blob (layer or config) by digest. HTTP GET /v2/{repository}/blobs/{digest}.
   *
   * <p>The response body is returned as an InputStream for streaming consumption. Caller is
   * responsible for closing the stream.
   *
   * @param repository the repository name
   * @param digest the blob digest (e.g., "sha256:...")
   * @return InputStream of blob content (caller must close)
   * @throws ResourceNotFoundException if blob not found (404)
   * @throws UnAuthorizedException if authentication fails (401/403)
   * @throws UnknownException if the request fails
   */
  public InputStream downloadBlob(String repository, String digest) {
    String url = String.format("%s/v2/%s/blobs/%s", registryEndpoint, repository, digest);

    HttpGet request = new HttpGet(url);
    String authHeader = getHttpAuthHeader(repository);
    if (authHeader != null) {
      request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
    }

    try {
      CloseableHttpResponse response = httpClient.execute(request);
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode != HttpStatus.SC_OK) {
        String errorBody =
            response.getEntity() != null
                ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)
                : StringUtils.EMPTY;
        response.close();
        String message =
            String.format(
                "Failed to download blob %s from %s - HTTP %d: %s",
                digest, repository, statusCode, errorBody);

        throw mapHttpStatusToException(statusCode, message);
      }

      if (response.getEntity() == null) {
        response.close();
        throw new UnknownException("Failed to download blob: empty response body");
      }

      // Return InputStream wrapped to close HTTP response on close()
      return new FilterInputStream(response.getEntity().getContent()) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            response.close();
          }
        }
      };
    } catch (IOException e) {
      throw new UnknownException("Failed to download blob", e);
    }
  }

  public String getRegistryEndpoint() {
    return registryEndpoint;
  }

  /** Maps HTTP status codes to SDK exceptions. */
  private RuntimeException mapHttpStatusToException(int statusCode, String message) {
    switch (statusCode) {
      case HttpStatus.SC_NOT_FOUND:
        return new ResourceNotFoundException(message);
      case HttpStatus.SC_UNAUTHORIZED:
      case HttpStatus.SC_FORBIDDEN:
        return new UnAuthorizedException(message);
      default:
        return new UnknownException(message);
    }
  }

  @Override
  public void close() throws Exception {
    if (httpClient != null) {
      httpClient.close();
    }
  }
}
