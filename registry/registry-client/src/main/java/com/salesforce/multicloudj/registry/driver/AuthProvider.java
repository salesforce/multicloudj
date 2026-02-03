package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;

/**
 * Authentication provider for registry requests.
 * Implemented by AbstractRegistry; each cloud implements getAuthUsername() and getAuthToken().
 */
public interface AuthProvider {

    String getAuthUsername() throws IOException;

    String getAuthToken() throws IOException;
}
