package com.salesforce.multicloudj.registry.driver;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * HTTP client for OCI Registry API v2 operations.
 * Handles authentication and standard Docker Registry API calls.
 */
class OciRegistryClient {
    private final String registryEndpoint;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;
    private final BearerTokenExchange tokenExchange;
    private AuthChallenge challenge;

    /**
     * Authentication provider interface for OciRegistryClient.
     * Implemented by AbstractRegistry, which delegates to Provider layer.
     */
    interface AuthProvider {
        /**
         * Returns authentication credentials (username and token).
         * This is the primary method for getting auth credentials.
         * Provider implementations can optimize by fetching both in a single API call
         * (e.g., Ali ACR returns both username and password from one API call).
         * 
         * Default implementation calls getAuthUsername() and getAuthToken() separately.
         * 
         * @return AuthCredentials containing username and token
         * @throws IOException if credentials cannot be obtained
         */
        default AuthCredentials getAuthCredentials() throws IOException {
            return new AuthCredentials(getAuthUsername(), getAuthToken());
        }
        
        /**
         * Returns the authentication username.
         * Implemented by Provider layer (e.g., "AWS", "oauth2accesstoken", "Ali").
         * 
         * @return Username for authentication
         * @throws IOException if username cannot be obtained
         */
        String getAuthUsername() throws IOException;
        
        /**
         * Returns the authentication token.
         * Implemented by Provider layer (e.g., ECR token, OAuth2 identity token).
         * 
         * @return Authentication token
         * @throws IOException if token cannot be obtained
         */
        String getAuthToken() throws IOException;
    }
    
    /**
     * Container for authentication credentials.
     */
    static class AuthCredentials {
        private final String username;
        private final String token;
        
        AuthCredentials(String username, String token) {
            this.username = username;
            this.token = token;
        }
        
        String getUsername() {
            return username;
        }
        
        String getToken() {
            return token;
        }
    }

    OciRegistryClient(String registryEndpoint, AuthProvider authProvider) {
        this(registryEndpoint, authProvider, null);
    }

    OciRegistryClient(String registryEndpoint, AuthProvider authProvider, CloseableHttpClient httpClient) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        this.httpClient = httpClient != null ? httpClient : HttpClients.createDefault();
        this.tokenExchange = new BearerTokenExchange(registryEndpoint, authProvider);
    }

    private void initializeAuth() throws IOException {
        if (challenge == null) {
            challenge = tokenExchange.ping();
        }
    }

    /**
     * Returns a formatted HTTP Authorization header string.
     * Formats credentials as "Basic ..." or "Bearer ..." based on registry's auth scheme.
     */
    private String getHttpAuthHeader(String repository) throws IOException {
        if (challenge == null) {
            initializeAuth();
        }

        // Get credentials in one call (allows Provider to optimize, e.g., Ali ACR)
        AuthCredentials credentials = authProvider.getAuthCredentials();
        String username = credentials.getUsername();
        String token = credentials.getToken();

        if (challenge.getScheme() == null || 
            challenge.getScheme().isEmpty() ||
            challenge.getScheme().equalsIgnoreCase("Basic")) {
            // Format as Basic Auth header
            String credentialsStr = username + ":" + token;
            String encoded = java.util.Base64.getEncoder()
                .encodeToString(credentialsStr.getBytes(StandardCharsets.UTF_8));
            return "Basic " + encoded;
        }

        if (challenge.getScheme().equalsIgnoreCase("Bearer")) {
            // Exchange identity token for Bearer token
            List<String> scopes = java.util.Arrays.asList("repository:" + repository + ":pull");
            String bearerToken = tokenExchange.getBearerToken(challenge, scopes);
            return "Bearer " + bearerToken;
        }

        // Default to Basic Auth
        String credentialsStr = username + ":" + token;
        String encoded = java.util.Base64.getEncoder()
            .encodeToString(credentialsStr.getBytes(StandardCharsets.UTF_8));
        return "Basic " + encoded;
    }

    /**
     * Fetches the manifest for the given reference (tag or digest).
     */
    Manifest fetchManifest(String repository, String reference) throws IOException {
        String url = String.format("%s/v2/%s/manifests/%s", registryEndpoint, repository, reference);
        HttpGet request = new HttpGet(url);
        request.setHeader(HttpHeaders.ACCEPT, String.join(",",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json"
        ));
        request.setHeader(HttpHeaders.AUTHORIZATION, getHttpAuthHeader(repository));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return parseManifestResponse(response);
            }
            throw new IOException("Failed to fetch manifest: " + response.getStatusLine().getStatusCode());
        }
    }

    private Manifest parseManifestResponse(CloseableHttpResponse response) throws IOException {
        String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        JsonObject manifestJson = JsonParser.parseString(json).getAsJsonObject();
        
        String mediaType = null;
        if (response.getFirstHeader(HttpHeaders.CONTENT_TYPE) != null) {
            mediaType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
        }
        if (mediaType == null && manifestJson.has("mediaType")) {
            mediaType = manifestJson.get("mediaType").getAsString();
        }
        
        if (isImageIndex(mediaType, manifestJson)) {
            JsonArray manifests = manifestJson.getAsJsonArray("manifests");
            return Manifest.index(manifests);
        } else {
            String configDigest = manifestJson.getAsJsonObject("config").get("digest").getAsString();
            JsonArray layers = manifestJson.getAsJsonArray("layers");
            List<String> layerDigests = new ArrayList<>();
            for (JsonElement layer : layers) {
                layerDigests.add(layer.getAsJsonObject().get("digest").getAsString());
            }
            return Manifest.image(configDigest, layerDigests);
        }
    }
    
    private boolean isImageIndex(String mediaType, JsonObject manifestJson) {
        if (mediaType != null) {
            return mediaType.contains("image.index") || mediaType.contains("manifest.list");
        }
        return manifestJson.has("manifests") && !manifestJson.has("layers");
    }

    /**
     * Downloads a blob (layer or config) by digest.
     */
    InputStream downloadBlob(String repository, String digest) throws IOException {
        String url = String.format("%s/v2/%s/blobs/%s", registryEndpoint, repository, digest);
        HttpGet request = new HttpGet(url);
        request.setHeader(HttpHeaders.AUTHORIZATION, getHttpAuthHeader(repository));
        
        CloseableHttpResponse response = httpClient.execute(request);
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
            return response.getEntity().getContent();
        }
        response.close();
        throw new IOException("Failed to download blob " + digest);
    }

    void close() throws IOException {
        if (tokenExchange != null) {
            tokenExchange.close();
        }
        httpClient.close();
    }

    static class Manifest {
        final String configDigest;
        final List<String> layerDigests;
        final JsonArray manifests;
        final boolean isIndex;

        Manifest(String configDigest, List<String> layerDigests) {
            this.configDigest = configDigest;
            this.layerDigests = layerDigests;
            this.manifests = null;
            this.isIndex = false;
        }

        Manifest(JsonArray manifests) {
            this.configDigest = null;
            this.layerDigests = null;
            this.manifests = manifests;
            this.isIndex = true;
        }

        static Manifest image(String configDigest, List<String> layerDigests) {
            return new Manifest(configDigest, layerDigests);
        }

        static Manifest index(JsonArray manifests) {
            return new Manifest(manifests);
        }

        boolean isIndex() {
            return isIndex;
        }

        JsonArray getManifests() {
            return manifests;
        }
    }
}
