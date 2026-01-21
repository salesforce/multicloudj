package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an authentication challenge from the registry.
 * Similar to go-containerregistry's transport.Challenge.
 */
@Builder
@Getter
public class AuthChallenge {
    /**
     * Authentication scheme: "Basic" or "Bearer"
     */
    private final String scheme;
    
    /**
     * Challenge parameters (e.g., realm, service, scope)
     */
    private final Map<String, String> parameters;
    
    /**
     * Whether the registry uses insecure (HTTP) connection
     */
    private final boolean insecure;
    
    /**
     * Parses WWW-Authenticate header value.
     * Format: "Bearer realm=\"https://auth.example.com/token\",service=\"registry.example.com\""
     */
    public static AuthChallenge parse(String wwwAuthenticate) {
        if (wwwAuthenticate == null || wwwAuthenticate.trim().isEmpty()) {
            return AuthChallenge.builder()
                .scheme("")
                .parameters(new HashMap<>())
                .insecure(false)
                .build();
        }
        
        String[] parts = wwwAuthenticate.split(" ", 2);
        String scheme = parts.length > 0 ? parts[0] : "";
        Map<String, String> params = new HashMap<>();
        
        if (parts.length > 1) {
            // Parse parameters: realm="...",service="..."
            String paramString = parts[1];
            String[] paramPairs = paramString.split(",");
            for (String pair : paramPairs) {
                String[] kv = pair.split("=", 2);
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim().replaceAll("^\"|\"$", ""); // Remove quotes
                    params.put(key, value);
                }
            }
        }
        
        return AuthChallenge.builder()
            .scheme(scheme)
            .parameters(params)
            .insecure(false)
            .build();
    }
    
    public String getRealm() {
        return parameters.getOrDefault("realm", "");
    }
    
    public String getService() {
        return parameters.getOrDefault("service", "");
    }
    
    public String getScope() {
        return parameters.getOrDefault("scope", "");
    }
}
