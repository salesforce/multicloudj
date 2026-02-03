package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;

/**
 * Authentication provider for registry requests.
 * Implemented by AbstractRegistry; each cloud implements getAuthUsername() and getAuthToken().
 */
public interface AuthProvider {

    /**
     * Returns username and token. Default: calls getAuthUsername() and getAuthToken().
     * Providers can override to fetch both in one call (e.g. Ali ACR).
     */
    default AuthCredentials getAuthCredentials() throws IOException {
        return new AuthCredentials(getAuthUsername(), getAuthToken());
    }

    String getAuthUsername() throws IOException;

    String getAuthToken() throws IOException;
}
