package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;

/**
 * Complete example showing how Ping discovers authentication method.
 * 
 * Flow:
 * 1. Client sends: GET https://registry.com/v2/
 * 2. Registry responds with authentication requirements
 * 3. Client parses response and decides authentication method
 * 4. Client uses appropriate authentication for subsequent requests
 */
public class PingExample {
    
    /**
     * Example 1: GCP Artifact Registry (requires Bearer Token)
     */
    public static void exampleGCP() throws IOException {
        System.out.println("=== GCP Artifact Registry Ping ===");
        
        RegistryPing ping = new RegistryPing("https://us-central1-docker.pkg.dev");
        
        // Step 1: Ping the registry
        AuthChallenge challenge = ping.ping();
        
        // Step 2: Check what authentication is required
        System.out.println("Response from registry:");
        System.out.println("  Status: 401 Unauthorized");
        System.out.println("  WWW-Authenticate: Bearer realm=\"https://oauth2.googleapis.com/token\",service=\"us-central1-docker.pkg.dev\"");
        System.out.println();
        System.out.println("Parsed Challenge:");
        System.out.println("  Scheme: " + challenge.getScheme());  // "Bearer"
        System.out.println("  Realm: " + challenge.getRealm());     // "https://oauth2.googleapis.com/token"
        System.out.println("  Service: " + challenge.getService()); // "us-central1-docker.pkg.dev"
        System.out.println();
        
        // Step 3: Decision - Need Bearer Token exchange
        if ("Bearer".equalsIgnoreCase(challenge.getScheme())) {
            System.out.println("Decision: Need Bearer Token exchange");
            System.out.println("  Next step: Exchange OAuth2 token for Bearer Token");
            System.out.println("  URL: " + challenge.getRealm() + "?service=" + challenge.getService());
        }
        
        ping.close();
    }
    
    /**
     * Example 2: AWS ECR (might accept Basic Auth directly)
     */
    public static void exampleAWS() throws IOException {
        System.out.println("\n=== AWS ECR Ping ===");
        
        RegistryPing ping = new RegistryPing("https://123456789012.dkr.ecr.us-east-1.amazonaws.com");
        
        AuthChallenge challenge = ping.ping();
        
        System.out.println("Response from registry:");
        if (challenge.getScheme().isEmpty()) {
            System.out.println("  Status: 200 OK");
            System.out.println("  No authentication required");
        } else {
            System.out.println("  Status: 401 Unauthorized");
            System.out.println("  WWW-Authenticate: " + challenge.getScheme());
        }
        System.out.println();
        System.out.println("Parsed Challenge:");
        System.out.println("  Scheme: " + (challenge.getScheme().isEmpty() ? "(none)" : challenge.getScheme()));
        System.out.println();
        
        // Decision - Can use Basic Auth directly
        if (challenge.getScheme().isEmpty() || "Basic".equalsIgnoreCase(challenge.getScheme())) {
            System.out.println("Decision: Can use Basic Auth directly");
            System.out.println("  Next step: Use AWS ECR token as Basic Auth");
            System.out.println("  Format: Basic base64(AWS:ecr-token)");
        }
        
        ping.close();
    }
    
    /**
     * Example 3: Docker Hub (requires Bearer Token)
     */
    public static void exampleDockerHub() throws IOException {
        System.out.println("\n=== Docker Hub Ping ===");
        
        RegistryPing ping = new RegistryPing("https://registry-1.docker.io");
        
        AuthChallenge challenge = ping.ping();
        
        System.out.println("Response from registry:");
        System.out.println("  Status: 401 Unauthorized");
        System.out.println("  WWW-Authenticate: Bearer realm=\"https://auth.docker.io/token\",service=\"registry.docker.io\"");
        System.out.println();
        System.out.println("Parsed Challenge:");
        System.out.println("  Scheme: " + challenge.getScheme());  // "Bearer"
        System.out.println("  Realm: " + challenge.getRealm());     // "https://auth.docker.io/token"
        System.out.println("  Service: " + challenge.getService()); // "registry.docker.io"
        System.out.println();
        
        // Decision - Need Bearer Token exchange
        if ("Bearer".equalsIgnoreCase(challenge.getScheme())) {
            System.out.println("Decision: Need Bearer Token exchange");
            System.out.println("  Next step: Exchange username:password for Bearer Token");
            System.out.println("  URL: " + challenge.getRealm() + "?service=" + challenge.getService());
        }
        
        ping.close();
    }
    
    /**
     * Complete workflow showing how to use Ping result
     */
    public static void completeWorkflow() throws IOException {
        System.out.println("\n=== Complete Workflow ===");
        
        String registryUrl = "https://us-central1-docker.pkg.dev";
        
        // Step 1: Ping to discover authentication
        System.out.println("Step 1: Ping registry");
        RegistryPing ping = new RegistryPing(registryUrl);
        AuthChallenge challenge = ping.ping();
        System.out.println("  Discovered: " + challenge.getScheme() + " authentication required");
        
        // Step 2: Based on challenge, choose authentication method
        System.out.println("\nStep 2: Choose authentication method");
        if (challenge.getScheme().isEmpty()) {
            System.out.println("  → No authentication needed");
            // Continue with requests without auth
        } else if ("Basic".equalsIgnoreCase(challenge.getScheme())) {
            System.out.println("  → Use Basic Auth directly");
            // Use: Authorization: Basic base64(username:password)
        } else if ("Bearer".equalsIgnoreCase(challenge.getScheme())) {
            System.out.println("  → Need Bearer Token exchange");
            System.out.println("  → Token server: " + challenge.getRealm());
            System.out.println("  → Service: " + challenge.getService());
            
            // Step 3: Exchange for Bearer Token
            System.out.println("\nStep 3: Exchange for Bearer Token");
            System.out.println("  GET " + challenge.getRealm() + 
                "?service=" + challenge.getService() + 
                "&scope=repository:my-repo:pull");
            System.out.println("  Authorization: Bearer <oauth2-token>");
            System.out.println("  Response: {\"token\": \"eyJhbGciOiJSUzI1NiIs...\"}");
            
            // Step 4: Use Bearer Token in requests
            System.out.println("\nStep 4: Use Bearer Token in registry requests");
            System.out.println("  GET /v2/my-repo/manifests/latest");
            System.out.println("  Authorization: Bearer eyJhbGciOiJSUzI1NiIs...");
        }
        
        ping.close();
    }
    
    public static void main(String[] args) throws IOException {
        exampleGCP();
        exampleAWS();
        exampleDockerHub();
        completeWorkflow();
    }
}
