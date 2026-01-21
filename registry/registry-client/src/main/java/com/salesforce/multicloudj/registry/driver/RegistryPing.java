package com.salesforce.multicloudj.registry.driver;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pings a Docker registry to discover authentication requirements.
 * 
 * This is the first step in the authentication flow:
 * 1. Send GET /v2/ to the registry
 * 2. Parse the response to determine authentication method
 * 3. Return AuthChallenge with scheme and parameters
 * 
 * Example responses:
 * - 200 OK → No authentication needed
 * - 401 Unauthorized + WWW-Authenticate: Bearer realm="...",service="..." → Need Bearer Token
 * - 401 Unauthorized + WWW-Authenticate: Basic → Need Basic Auth
 */
public class RegistryPing {
    
    private final String registryEndpoint;
    private final CloseableHttpClient httpClient;
    
    public RegistryPing(String registryEndpoint) {
        this.registryEndpoint = registryEndpoint;
        this.httpClient = HttpClients.createDefault();
    }
    
    /**
     * Pings the registry by sending GET /v2/
     * 
     * @return AuthChallenge containing authentication requirements
     * @throws IOException if ping fails
     */
    public AuthChallenge ping() throws IOException {
        // Step 1: Build the ping URL
        // Format: https://registry.example.com/v2/
        String pingUrl = registryEndpoint;
        if (!pingUrl.endsWith("/")) {
            pingUrl += "/";
        }
        if (!pingUrl.endsWith("/v2/")) {
            pingUrl += "v2/";
        }
        
        // Step 2: Send GET request (no authentication yet!)
        HttpGet request = new HttpGet(pingUrl);
        // Don't set any Authorization header - we want to see what the registry requires
        
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            int statusCode = response.getStatusLine().getStatusCode();
            
            // Step 3: Parse response based on status code
            switch (statusCode) {
                case HttpStatus.SC_OK:
                    // 200 OK - No authentication required
                    return AuthChallenge.builder()
                        .scheme("")
                        .parameters(new HashMap<>())
                        .insecure(false)
                        .build();
                
                case HttpStatus.SC_UNAUTHORIZED:
                    // 401 Unauthorized - Authentication required
                    // Parse WWW-Authenticate header
                    return parseWWWAuthenticate(response);
                
                default:
                    // Other status codes - might be an error
                    String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    throw new IOException("Unexpected response from registry ping: " + 
                        statusCode + " - " + body);
            }
        }
    }
    
    /**
     * Parses the WWW-Authenticate header from 401 response.
     * 
     * Examples:
     * - "Bearer realm=\"https://auth.example.com/token\",service=\"registry.example.com\""
     * - "Basic"
     * - "Bearer realm=\"https://oauth2.googleapis.com/token\",service=\"gcr.io\",scope=\"repository:my-repo:pull\""
     */
    private AuthChallenge parseWWWAuthenticate(CloseableHttpResponse response) throws IOException {
        // Get WWW-Authenticate header
        org.apache.http.Header wwwAuthHeader = response.getFirstHeader(HttpHeaders.WWW_AUTHENTICATE);
        
        if (wwwAuthHeader == null || wwwAuthHeader.getValue() == null) {
            // No WWW-Authenticate header - return empty challenge
            return AuthChallenge.builder()
                .scheme("")
                .parameters(new HashMap<>())
                .insecure(false)
                .build();
        }
        
        String wwwAuth = wwwAuthHeader.getValue();
        
        // Parse the header value
        // Format: "Scheme param1=\"value1\",param2=\"value2\""
        String[] parts = wwwAuth.split(" ", 2);
        String scheme = parts.length > 0 ? parts[0].trim() : "";
        Map<String, String> parameters = new HashMap<>();
        
        // Parse parameters if present
        if (parts.length > 1) {
            String paramString = parts[1];
            // Parse: realm="...",service="...",scope="..."
            // Use regex to extract key-value pairs
            Pattern pattern = Pattern.compile("(\\w+)=\"([^\"]+)\"");
            Matcher matcher = pattern.matcher(paramString);
            
            while (matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2);
                parameters.put(key, value);
            }
        }
        
        return AuthChallenge.builder()
            .scheme(scheme)
            .parameters(parameters)
            .insecure(false) // TODO: Detect from URL scheme
            .build();
    }
    
    public void close() throws IOException {
        httpClient.close();
    }
    
    /**
     * Example usage and test cases
     */
    public static void main(String[] args) throws IOException {
        // Example 1: GCP Artifact Registry
        System.out.println("=== Example 1: GCP Artifact Registry ===");
        RegistryPing gcpPing = new RegistryPing("https://us-central1-docker.pkg.dev");
        try {
            AuthChallenge challenge = gcpPing.ping();
            System.out.println("Scheme: " + challenge.getScheme());
            System.out.println("Realm: " + challenge.getRealm());
            System.out.println("Service: " + challenge.getService());
            System.out.println("Scope: " + challenge.getScope());
            // Expected:
            // Scheme: Bearer
            // Realm: https://oauth2.googleapis.com/token
            // Service: us-central1-docker.pkg.dev
        } finally {
            gcpPing.close();
        }
        
        // Example 2: Docker Hub
        System.out.println("\n=== Example 2: Docker Hub ===");
        RegistryPing dockerPing = new RegistryPing("https://registry-1.docker.io");
        try {
            AuthChallenge challenge = dockerPing.ping();
            System.out.println("Scheme: " + challenge.getScheme());
            System.out.println("Realm: " + challenge.getRealm());
            // Expected:
            // Scheme: Bearer
            // Realm: https://auth.docker.io/token
        } finally {
            dockerPing.close();
        }
        
        // Example 3: AWS ECR (might return 200 or 401 depending on setup)
        System.out.println("\n=== Example 3: AWS ECR ===");
        RegistryPing ecrPing = new RegistryPing("https://123456789012.dkr.ecr.us-east-1.amazonaws.com");
        try {
            AuthChallenge challenge = ecrPing.ping();
            System.out.println("Scheme: " + challenge.getScheme());
            if (challenge.getScheme().isEmpty()) {
                System.out.println("No authentication required (200 OK)");
            } else {
                System.out.println("Realm: " + challenge.getRealm());
            }
        } finally {
            ecrPing.close();
        }
    }
}
