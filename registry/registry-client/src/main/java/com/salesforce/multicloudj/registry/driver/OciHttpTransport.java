package com.salesforce.multicloudj.registry.driver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.salesforce.multicloudj.registry.model.Manifest;
import com.salesforce.multicloudj.registry.model.Platform;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** OCI Registry API v2 HTTP client; handles auth and registry operations. */
public class OciHttpTransport implements AutoCloseable {

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
     * Creates a new OciHttpTransport with no interceptors.
     *
     * @param registryEndpoint the registry base URL
     * @param authProvider the authentication provider
     */
    public OciHttpTransport(String registryEndpoint, AuthProvider authProvider) {
        this(registryEndpoint, authProvider, (List<HttpRequestInterceptor>) null);
    }

    /**
     * Creates OciHttpTransport with specified HTTP interceptors.
     *
     * @param registryEndpoint the registry base URL
     * @param authProvider the authentication provider
     * @param interceptors list of HTTP request interceptors (null or empty for default client)
     */
    public OciHttpTransport(String registryEndpoint, AuthProvider authProvider,
                            List<HttpRequestInterceptor> interceptors) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        
        if (interceptors == null || interceptors.isEmpty()) {
            this.httpClient = HttpClients.createDefault();
        } else {
            var builder = HttpClients.custom();
            for (HttpRequestInterceptor interceptor : interceptors) {
                builder.addInterceptorLast(interceptor);
            }
            this.httpClient = builder.build();
        }
        this.tokenExchange = new BearerTokenExchange(this.httpClient);
    }

    /**
     * Package-private constructor for testing with a mock HTTP client.
     *
     * @param registryEndpoint the registry base URL
     * @param authProvider the authentication provider
     * @param httpClient the HTTP client to use
     */
    OciHttpTransport(String registryEndpoint, AuthProvider authProvider,
                     CloseableHttpClient httpClient) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        this.httpClient = httpClient;
        this.tokenExchange = new BearerTokenExchange(this.httpClient);
    }


    /**
     * Builds HTTP Authorization header for the given repository.
     * Discovers auth requirements on first call and caches the result.
     *
     * @param repository the repository name (used for Bearer token scope)
     * @return the Authorization header value, or null if anonymous access (no auth required)
     * @throws IOException if authentication fails
     */
    public String getHttpAuthHeader(String repository) throws IOException {
        // Double-checked locking for thread-safe lazy initialization
        if (cachedChallenge == null) {
            synchronized (challengeLock) {
                if (cachedChallenge == null) {
                    cachedChallenge = AuthChallenge.discover(httpClient, registryEndpoint);
                }
            }
        }

        final AuthScheme scheme = cachedChallenge.getScheme();

        switch (scheme) {
            case ANONYMOUS:
                return null;
            case BASIC:
                return buildBasicAuthHeader();
            case BEARER:
                return buildBearerAuthHeader(repository);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported authentication scheme: " + scheme);
        }
    }

    private String buildBasicAuthHeader() throws IOException {
        String username = authProvider.getAuthUsername();
        String token = authProvider.getAuthToken();
        String credentials = username + ":" + token;
        String encoded = Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    private String buildBearerAuthHeader(String repository) throws IOException {
        // Get identity token from provider (e.g., GCP OAuth2 access token)
        String identityToken = authProvider.getAuthToken();

        // Exchange for registry-scoped bearer token
        String bearerToken = tokenExchange.getBearerToken(
                cachedChallenge, identityToken, repository, "pull");

        return "Bearer " + bearerToken;
    }

    /**
     * Fetches the image manifest for the given repository and reference (tag or digest).
     * HTTP GET /v2/{repository}/manifests/{reference}.
     *
     * @param repository the repository name (e.g., "my-repo/my-image")
     * @param reference the tag or digest (e.g., "latest" or "sha256:...")
     * @return the parsed Manifest object
     * @throws IOException if the request fails or manifest cannot be parsed
     */
    public Manifest fetchManifest(String repository, String reference) throws IOException {
        String url = String.format("%s/v2/%s/manifests/%s", registryEndpoint, repository, reference);

        HttpGet request = new HttpGet(url);
        request.setHeader(HttpHeaders.ACCEPT, MANIFEST_ACCEPT_HEADER);
        String authHeader = getHttpAuthHeader(repository);
        if (authHeader != null) {
            request.setHeader(HttpHeaders.AUTHORIZATION, authHeader);
        }

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                String errorBody = response.getEntity() != null
                        ? EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8)
                        : StringUtils.EMPTY;
                throw new IOException(String.format(
                        "Failed to fetch manifest for %s:%s from %s - HTTP %d: %s",
                        repository, reference, registryEndpoint, statusCode, errorBody));
            }

            if (response.getEntity() == null) {
                throw new IOException("Failed to fetch manifest: empty response body");
            }

            // Check manifest size limit (100 MB)
            long contentLength = response.getEntity().getContentLength();
            if (contentLength > MAX_MANIFEST_SIZE_BYTES) {
                throw new IOException(String.format(
                        "Manifest size (%d bytes) exceeds maximum allowed size (%d bytes)",
                        contentLength, MAX_MANIFEST_SIZE_BYTES));
            }

            String responseBody = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            Header header = response.getFirstHeader("Docker-Content-Digest");
            String digestHeader = header != null ? header.getValue() : null;

            // If Docker-Content-Digest header is missing (e.g., AWS ECR), calculate it from the response body
            if (digestHeader == null) {
                try {
                    MessageDigest md = MessageDigest.getInstance(DIGEST_ALGORITHM);
                    byte[] hash = md.digest(responseBody.getBytes(StandardCharsets.UTF_8));
                    digestHeader = DIGEST_PREFIX + Hex.encodeHexString(hash);
                } catch (NoSuchAlgorithmException e) {
                    throw new IOException("Failed to calculate manifest digest: " + DIGEST_ALGORITHM
                            + " SHA-256 algorithm not available", e);
                }
            }

            // Validate digest if fetching by digest
            if (reference.startsWith(DIGEST_PREFIX)) {
                if (!reference.equals(digestHeader)) {
                    throw new IOException(String.format(
                            "Manifest digest mismatch: expected %s, got %s for %s:%s",
                            reference, digestHeader, repository, reference));
                }
            }

            return parseManifestResponse(responseBody, digestHeader);
        }
    }

    /**
     * Parses the manifest JSON response into a Manifest object.
     * Handles both image manifests and image indexes (multi-arch).
     */
    private Manifest parseManifestResponse(String responseBody, String digest) throws IOException {
        JsonObject json;
        try {
            json = JsonParser.parseString(responseBody).getAsJsonObject();
        } catch (JsonSyntaxException e) {
            throw new IOException("Invalid JSON response from registry: " + 
                    responseBody.substring(0, Math.min(200, responseBody.length())), e);
        }
        
        String mediaType = getStringOrNull(json, OciManifestFields.MEDIA_TYPE);
        if (mediaType == null) {
            mediaType = "";
        }

        // Check if this is an image index (multi-arch)
        if (OciManifestFields.MEDIA_TYPE_OCI_INDEX.equals(mediaType) 
                || OciManifestFields.MEDIA_TYPE_DOCKER_MANIFEST_LIST.equals(mediaType)
                || json.has(OciManifestFields.MANIFESTS)) {
            return parseImageIndex(json, digest);
        }

        // Single image manifest
        return parseImageManifest(json, digest);
    }

    private static String getStringOrNull(JsonObject json, String field) {
        return json.has(field) ? json.get(field).getAsString() : null;
    }

    private static Long getLongOrNull(JsonObject json, String field) {
        return json.has(field) ? json.get(field).getAsLong() : null;
    }

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

    /**
     * Parses subject field from a manifest or index JSON.
     */
    private String parseSubject(JsonObject json) {
        if (!json.has(OciManifestFields.SUBJECT)) {
            return null;
        }
        JsonObject subjectObj = json.getAsJsonObject(OciManifestFields.SUBJECT);
        return getStringOrNull(subjectObj, OciManifestFields.DIGEST);
    }


    /**
     * Parses platform information from a manifest descriptor in an image index.
     * Returns null if no platform field exists.
     *
     * @param manifestDesc the manifest descriptor JSON object
     * @return Platform object or null if no platform information
     */
    private Platform parsePlatform(JsonObject manifestDesc) {
        if (!manifestDesc.has(OciManifestFields.PLATFORM)) {
            return null;
        }

        JsonObject platformObj = manifestDesc.getAsJsonObject(OciManifestFields.PLATFORM);
        String operatingSystem = getStringOrNull(platformObj, OciManifestFields.OS);
        String architecture = getStringOrNull(platformObj, OciManifestFields.ARCHITECTURE);
        String osVersion = getStringOrNull(platformObj, OciManifestFields.OS_VERSION);
        String variant = getStringOrNull(platformObj, OciManifestFields.VARIANT);

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
     * Parses os.features array from a platform object.
     * Returns null if no os.features field exists.
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

    /**
     * Parses an image index (multi-architecture manifest list).
     */
    private Manifest parseImageIndex(JsonObject json, String digest) throws IOException {
        JsonArray manifestsArray = json.getAsJsonArray(OciManifestFields.MANIFESTS);
        int initialCapacity = manifestsArray != null ? manifestsArray.size() : 0;
        List<Manifest.IndexEntry> entries = new ArrayList<>(initialCapacity);

        if (manifestsArray == null) {
            return Manifest.index(entries, digest);
        }

        for (JsonElement element : manifestsArray) {
            JsonObject manifestDesc = element.getAsJsonObject();
            if (!manifestDesc.has(OciManifestFields.DIGEST)) {
                throw new IOException("Invalid image index: manifest entry missing required 'digest' field");
            }
            String manifestDigest = getStringOrNull(manifestDesc, OciManifestFields.DIGEST);
            Platform platform = parsePlatform(manifestDesc);
            entries.add(new Manifest.IndexEntry(manifestDigest, platform));
        }

        return Manifest.index(entries, digest, parseAnnotations(json), parseSubject(json));
    }

    /**
     * Parses a single image manifest.
     */
    private Manifest parseImageManifest(JsonObject json, String digest) throws IOException {
        // Get config digest
        String configDigest = null;
        if (json.has(OciManifestFields.CONFIG)) {
            JsonObject config = json.getAsJsonObject(OciManifestFields.CONFIG);
            if (config != null) {
                configDigest = getStringOrNull(config, OciManifestFields.DIGEST);
            }
        }

        List<Manifest.LayerInfo> layerInfos = parseLayerInfos(json);
        return Manifest.image(configDigest, layerInfos, digest, parseAnnotations(json), parseSubject(json));
    }

    /**
     * Parses layer information from a manifest JSON object.
     */
    private List<Manifest.LayerInfo> parseLayerInfos(JsonObject json) throws IOException {
        JsonArray layers = json.has(OciManifestFields.LAYERS) ? json.getAsJsonArray(OciManifestFields.LAYERS) : null;
        int layerCapacity = layers != null ? layers.size() : 0;
        List<Manifest.LayerInfo> layerInfos = new ArrayList<>(layerCapacity);
        if (layers != null) {
            for (JsonElement element : layers) {
                JsonObject layer = element.getAsJsonObject();
                if (!layer.has(OciManifestFields.DIGEST)) {
                    throw new IOException("Invalid image manifest: layer missing required 'digest' field");
                }
                String layerDigest = getStringOrNull(layer, OciManifestFields.DIGEST);
                String layerMediaType = getStringOrNull(layer, OciManifestFields.MEDIA_TYPE);
                Long size = getLongOrNull(layer, OciManifestFields.SIZE);
                layerInfos.add(new Manifest.LayerInfo(layerDigest, layerMediaType, size));
            }
        }
        return layerInfos;
    }

    /** Downloads a blob (layer or config) by digest. */
    public InputStream downloadBlob(String repository, String digest) throws IOException {
        // TODO: need to be implemented
        throw new UnsupportedOperationException("downloadBlob() not yet implemented");
    }

    public String getRegistryEndpoint() {
        return registryEndpoint;
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }
}
