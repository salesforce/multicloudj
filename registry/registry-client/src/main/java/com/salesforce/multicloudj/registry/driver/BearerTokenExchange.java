package com.salesforce.multicloudj.registry.driver;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles Bearer Token exchange for OCI Registry authentication.
 * Similar to go-containerregistry's transport.bearerTransport.
 * 
 * Flow:
 * 1. Ping registry to get Challenge
 * 2. Exchange Basic credentials for Bearer Token
 * 3. Use Bearer Token in subsequent requests
 * 4. Refresh token on 401
 */
class BearerTokenExchange {
    private final String registryEndpoint;
    private final CloseableHttpClient httpClient;
    private final AuthProvider authProvider;
    private String cachedToken;
    private long tokenExpiry;
    
    interface AuthProvider {
        String getBasicAuthHeader() throws IOException;
        String getIdentityToken() throws IOException; // For OAuth flow
    }
    
    BearerTokenExchange(String registryEndpoint, AuthProvider authProvider) {
        this.registryEndpoint = registryEndpoint;
        this.authProvider = authProvider;
        this.httpClient = HttpClients.createDefault();
    }
    
    /**
     * Pings the registry to discover authentication requirements.
     * Returns Challenge with scheme and parameters.
     */
    AuthChallenge ping() throws IOException {
        String url = registryEndpoint + "/v2/";
        HttpGet request = new HttpGet(url);
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            if (statusCode == HttpStatus.SC_OK) {
                // No authentication required
                return AuthChallenge.builder()
                    .scheme("")
                    .parameters(new java.util.HashMap<>())
                    .insecure(false)
                    .build();
            }
            
            if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
                // Parse WWW-Authenticate header
                String wwwAuth = response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE) != null
                    ? response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE).getValue()
                    : null;
                
                if (wwwAuth != null) {
                    return AuthChallenge.parse(wwwAuth);
                }
            }
            
            throw new IOException("Unexpected response from registry ping: " + statusCode);
        }
    }
    
    /**
     * Exchanges credentials for a Bearer Token.
     * Supports both Basic Auth and OAuth2 flows.
     */
    BearerToken exchange(AuthChallenge challenge, List<String> scopes) throws IOException {
        String realm = challenge.getRealm();
        if (realm.isEmpty()) {
            throw new IOException("Challenge missing realm parameter");
        }
        
        // Build token URL with parameters
        StringBuilder tokenUrl = new StringBuilder(realm);
        tokenUrl.append("?service=").append(URLEncoder.encode(challenge.getService(), StandardCharsets.UTF_8));
        
        if (!scopes.isEmpty()) {
            tokenUrl.append("&scope=").append(URLEncoder.encode(String.join(" ", scopes), StandardCharsets.UTF_8));
        }
        
        // Try OAuth2 flow first (if IdentityToken is available)
        String identityToken = authProvider.getIdentityToken();
        if (identityToken != null && !identityToken.isEmpty()) {
            try {
                return exchangeOAuth2(tokenUrl.toString(), identityToken);
            } catch (IOException e) {
                // Fall back to Basic Auth if OAuth2 fails
            }
        }
        
        // Use Basic Auth flow
        return exchangeBasic(tokenUrl.toString());
    }
    
    /**
     * Exchanges Basic credentials for Bearer Token.
     */
    private BearerToken exchangeBasic(String tokenUrl) throws IOException {
        HttpGet request = new HttpGet(tokenUrl);
        request.setHeader(HttpHeaders.AUTHORIZATION, authProvider.getBasicAuthHeader());
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                throw new IOException("Token exchange failed: " + statusCode + " " +
                    EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
            }
            
            String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            return parseTokenResponse(json);
        }
    }
    
    /**
     * Exchanges OAuth2 Identity Token for Bearer Token.
     */
    private BearerToken exchangeOAuth2(String tokenUrl, String identityToken) throws IOException {
        HttpPost request = new HttpPost(tokenUrl);
        request.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");
        
        // OAuth2 grant_type=refresh_token or grant_type=access_token
        String body = "grant_type=refresh_token&refresh_token=" + 
            URLEncoder.encode(identityToken, StandardCharsets.UTF_8);
        request.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == HttpStatus.SC_NOT_FOUND) {
                // OAuth2 endpoint not available, fall back to Basic
                throw new IOException("OAuth2 endpoint not found");
            }
            if (statusCode != HttpStatus.SC_OK) {
                throw new IOException("OAuth2 token exchange failed: " + statusCode);
            }
            
            String json = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            return parseTokenResponse(json);
        }
    }
    
    /**
     * Parses token response JSON.
     * Handles both "token" and "access_token" fields.
     */
    private BearerToken parseTokenResponse(String json) throws IOException {
        JsonObject tokenJson = JsonParser.parseString(json).getAsJsonObject();
        
        String token = null;
        if (tokenJson.has("token")) {
            token = tokenJson.get("token").getAsString();
        } else if (tokenJson.has("access_token")) {
            token = tokenJson.get("access_token").getAsString();
        }
        
        if (token == null || token.isEmpty()) {
            throw new IOException("No token in response: " + json);
        }
        
        int expiresIn = tokenJson.has("expires_in") 
            ? tokenJson.get("expires_in").getAsInt() 
            : 3600; // Default 1 hour
        
        return new BearerToken(token, expiresIn);
    }
    
    /**
     * Gets a valid Bearer Token, refreshing if necessary.
     */
    String getBearerToken(AuthChallenge challenge, List<String> scopes) throws IOException {
        // Check if cached token is still valid
        if (cachedToken != null && System.currentTimeMillis() < tokenExpiry) {
            return cachedToken;
        }
        
        // Exchange for new token
        BearerToken token = exchange(challenge, scopes);
        cachedToken = token.getToken();
        tokenExpiry = System.currentTimeMillis() + (token.getExpiresIn() * 1000L) - 60000; // Refresh 1 min early
        
        return cachedToken;
    }
    
    void close() throws IOException {
        httpClient.close();
    }
    
    static class BearerToken {
        private final String token;
        private final int expiresIn;
        
        BearerToken(String token, int expiresIn) {
            this.token = token;
            this.expiresIn = expiresIn;
        }
        
        String getToken() {
            return token;
        }
        
        int getExpiresIn() {
            return expiresIn;
        }
    }
}
