package com.salesforce.multicloudj.registry.driver;

/**
 * Container for registry auth (username + token for Basic or Bearer).
 */
public final class AuthCredentials {

    private final String username;
    private final String token;

    public AuthCredentials(String username, String token) {
        this.username = username;
        this.token = token;
    }

    public String getUsername() {
        return username;
    }

    public String getToken() {
        return token;
    }
}
