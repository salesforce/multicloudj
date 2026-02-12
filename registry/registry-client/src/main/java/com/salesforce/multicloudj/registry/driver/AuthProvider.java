package com.salesforce.multicloudj.registry.driver;

import java.io.IOException;

/**
 * Authentication provider for registry requests.
 */
public interface AuthProvider {

    String getAuthUsername() throws IOException;

    String getAuthToken() throws IOException;
}
