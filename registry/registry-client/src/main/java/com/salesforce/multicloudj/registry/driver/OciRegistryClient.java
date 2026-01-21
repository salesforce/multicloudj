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
 * 
 * This implementation follows go-containerregistry's approach:
 * 1. Ping registry to discover authentication requirements
 * 2. Exchange credentials for Bearer Token if needed (for GCP)
 * 3. Use appropriate authentication (Basic or Bearer) for requests
 * 4. Refresh tokens on 401 errors
 */
class OciRegistryClient {
    private final String registryEndpoint;
    private final String repository;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;
    private BearerTokenExchange tokenExchange;
    private AuthChallenge challenge;
    private final List<String> defaultScopes;

    interface AuthProvider {
        /**
         * Gets Basic Auth header (username:password encoded as base64).
         * Used for ECR/ACR or as initial credential for Bearer Token exchange.
         */
        String getBasicAuthHeader() throws IOException;
        
        /**
         * Gets Identity Token (OAuth2 access token) for Bearer Token exchange.
         * Used for GCP Artifact Registry.
         * Returns null if not available (e.g., for ECR/ACR).
         */
        String getIdentityToken() throws IOException;
    }

    OciRegistryClient(String registryEndpoint, String repository, AuthProvider authProvider) {
        this.registryEndpoint = registryEndpoint;
        this.repository = repository;
        this.authProvider = authProvider;
        this.httpClient = HttpClients.createDefault();
        // Create adapter to convert OciRegistryClient.AuthProvider to BearerTokenExchange.AuthProvider
        this.tokenExchange = new BearerTokenExchange(registryEndpoint, new BearerTokenExchange.AuthProvider() {
            @Override
            public String getBasicAuthHeader() throws IOException {
                return authProvider.getBasicAuthHeader();
            }

            @Override
            public String getIdentityToken() throws IOException {
                return authProvider.getIdentityToken();
            }
        });
        // Default scopes for pulling images
        this.defaultScopes = Arrays.asList("repository:" + repository + ":pull");
    }

    /**
     * Initializes authentication by pinging the registry.
     * This discovers the authentication scheme (Basic or Bearer).
     * Similar to go-containerregistry's transport.NewWithContext.
     */
    private void initializeAuth() throws IOException {
        if (challenge == null) {
            challenge = tokenExchange.ping();
        }
    }

    /**
     * Gets the appropriate Authorization header for a request.
     * Dynamically selects Basic Auth or Bearer Token based on registry challenge.
     */
    private String getAuthHeader() throws IOException {
        if (challenge == null) {
            initializeAuth();
        }

        // If no challenge or Basic scheme, use Basic Auth
        if (challenge.getScheme() == null || 
            challenge.getScheme().isEmpty() ||
            challenge.getScheme().equalsIgnoreCase("Basic")) {
            return authProvider.getBasicAuthHeader();
        }

        // For Bearer scheme, exchange for token
        if (challenge.getScheme().equalsIgnoreCase("Bearer")) {
            String token = tokenExchange.getBearerToken(challenge, defaultScopes);
            return "Bearer " + token;
        }

        // Fallback to Basic
        return authProvider.getBasicAuthHeader();
    }

    /**
     * Fetches the manifest for the given reference (tag or digest).
     */
    Manifest fetchManifest(String reference) throws IOException {
        String url = String.format("%s/v2/%s/manifests/%s", registryEndpoint, repository, reference);
        HttpGet request = new HttpGet(url);
        // Accept both image manifests and image indexes (multi-arch support)
        request.setHeader(HttpHeaders.ACCEPT, String.join(",",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json"
        ));
        
        // Set auth header
        request.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            // Handle 401 - token might be expired, refresh and retry
            if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                // Clear cached token and retry
                challenge = null; // Force re-initialization
                request.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
                
                try (CloseableHttpResponse retryResponse = httpClient.execute(request)) {
                    if (retryResponse.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                        throw new IOException("Failed to fetch manifest after auth refresh: " + 
                            retryResponse.getStatusLine().getStatusCode() + " " +
                            EntityUtils.toString(retryResponse.getEntity(), StandardCharsets.UTF_8));
                    }
                    return parseManifestResponse(retryResponse);
                }
            }
            
            if (statusCode == HttpStatus.SC_OK) {
                return parseManifestResponse(response);
            }
            
            throw new IOException("Failed to fetch manifest: " + statusCode + " " + 
                EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
        }
    }

    private Manifest parseManifestResponse(CloseableHttpResponse response) throws IOException {
        String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        JsonObject manifestJson = JsonParser.parseString(json).getAsJsonObject();
        
        // Read mediaType from Content-Type header or JSON body
        String mediaType = null;
        if (response.getFirstHeader(HttpHeaders.CONTENT_TYPE) != null) {
            mediaType = response.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue();
        }
        if (mediaType == null && manifestJson.has("mediaType")) {
            mediaType = manifestJson.get("mediaType").getAsString();
        }
        
        // Check if this is an image index (multi-arch)
        if (isImageIndex(mediaType, manifestJson)) {
            // Return index manifest with manifests array
            JsonArray manifests = manifestJson.getAsJsonArray("manifests");
            return Manifest.index(manifests);
        } else {
            // Regular image manifest
            String configDigest = manifestJson.getAsJsonObject("config").get("digest").getAsString();
            JsonArray layers = manifestJson.getAsJsonArray("layers");
            
            List<String> layerDigests = new ArrayList<>();
            for (JsonElement layer : layers) {
                String digest = layer.getAsJsonObject().get("digest").getAsString();
                layerDigests.add(digest);
            }
            
            return Manifest.image(configDigest, layerDigests);
        }
    }
    
    private boolean isImageIndex(String mediaType, JsonObject manifestJson) {
        if (mediaType != null) {
            return mediaType.contains("image.index") || mediaType.contains("manifest.list");
        }
        // Fallback: check if JSON has "manifests" array (index) vs "layers" array (image)
        return manifestJson.has("manifests") && !manifestJson.has("layers");
    }

    /**
     * Downloads a blob (layer or config) by digest.
     */
    InputStream downloadBlob(String digest) throws IOException {
        String url = String.format("%s/v2/%s/blobs/%s", registryEndpoint, repository, digest);
        HttpGet request = new HttpGet(url);
        request.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
        
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        
        // Handle 401 - refresh token and retry
        if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
            response.close();
            challenge = null; // Force re-initialization
            request.setHeader(HttpHeaders.AUTHORIZATION, getAuthHeader());
            response = httpClient.execute(request);
            statusCode = response.getStatusLine().getStatusCode();
        }
        
        if (statusCode != HttpStatus.SC_OK) {
            response.close();
            throw new IOException("Failed to download blob " + digest + ": " + statusCode);
        }
        
        return response.getEntity().getContent();
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
        final JsonArray manifests; // For image index (multi-arch)
        final boolean isIndex;

        // Constructor for regular image manifest
        Manifest(String configDigest, List<String> layerDigests) {
            this.configDigest = configDigest;
            this.layerDigests = layerDigests;
            this.manifests = null;
            this.isIndex = false;
        }

        // Constructor for image index (multi-arch)
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
