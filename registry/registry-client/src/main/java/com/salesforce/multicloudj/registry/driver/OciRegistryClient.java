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
import java.util.List;

/**
 * HTTP client for OCI Registry API v2 operations.
 * Handles authentication and standard Docker Registry API calls.
 */
class OciRegistryClient {
    private final String registryEndpoint;
    private final String repository;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;

    interface AuthProvider {
        String getAuthHeader() throws IOException;
    }

    OciRegistryClient(String registryEndpoint, String repository, AuthProvider authProvider) {
        this.registryEndpoint = registryEndpoint;
        this.repository = repository;
        this.authProvider = authProvider;
        this.httpClient = HttpClients.createDefault();
    }

    /**
     * Fetches the manifest for the given reference (tag or digest).
     */
    Manifest fetchManifest(String reference) throws IOException {
        String url = String.format("%s/v2/%s/manifests/%s", registryEndpoint, repository, reference);
        HttpGet request = new HttpGet(url);
        request.setHeader(HttpHeaders.ACCEPT, "application/vnd.docker.distribution.manifest.v2+json,application/vnd.oci.image.manifest.v1+json");
        
        // Try with auth first
        request.setHeader(HttpHeaders.AUTHORIZATION, authProvider.getAuthHeader());
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                // Handle 401 - retry with fresh auth (in case token expired)
                request.setHeader(HttpHeaders.AUTHORIZATION, authProvider.getAuthHeader());
                try (CloseableHttpResponse retryResponse = httpClient.execute(request)) {
                    return parseManifestResponse(retryResponse);
                }
            } else if (statusCode == HttpStatus.SC_OK) {
                return parseManifestResponse(response);
            } else {
                throw new IOException("Failed to fetch manifest: " + statusCode + " " + 
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
            }
        }
    }

    private Manifest parseManifestResponse(CloseableHttpResponse response) throws IOException {
        String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        JsonObject manifestJson = JsonParser.parseString(json).getAsJsonObject();
        
        String configDigest = manifestJson.getAsJsonObject("config").get("digest").getAsString();
        JsonArray layers = manifestJson.getAsJsonArray("layers");
        
        List<String> layerDigests = new ArrayList<>();
        for (JsonElement layer : layers) {
            String digest = layer.getAsJsonObject().get("digest").getAsString();
            layerDigests.add(digest);
        }
        
        return new Manifest(configDigest, layerDigests);
    }

    /**
     * Downloads a blob (layer or config) by digest.
     */
    InputStream downloadBlob(String digest) throws IOException {
        String url = String.format("%s/v2/%s/blobs/%s", registryEndpoint, repository, digest);
        HttpGet request = new HttpGet(url);
        request.setHeader(HttpHeaders.AUTHORIZATION, authProvider.getAuthHeader());
        
        CloseableHttpResponse response = httpClient.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        
        if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
            response.close();
            // Retry with fresh auth
            request.setHeader(HttpHeaders.AUTHORIZATION, authProvider.getAuthHeader());
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
        httpClient.close();
    }

    static class Manifest {
        final String configDigest;
        final List<String> layerDigests;

        Manifest(String configDigest, List<String> layerDigests) {
            this.configDigest = configDigest;
            this.layerDigests = layerDigests;
        }
    }
}
