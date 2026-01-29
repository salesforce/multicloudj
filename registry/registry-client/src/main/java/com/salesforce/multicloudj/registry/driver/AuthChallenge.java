package com.salesforce.multicloudj.registry.driver;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents an HTTP WWW-Authenticate challenge.
 * Example: "Bearer realm=\"https://auth.docker.io/token\",service=\"registry.docker.io\""
 */
class AuthChallenge {
    private final String scheme;
    private final String realm;
    private final String service;

    private AuthChallenge(String scheme, String realm, String service) {
        this.scheme = scheme;
        this.realm = realm;
        this.service = service;
    }

    static AuthChallenge parse(String wwwAuthenticate) {
        if (wwwAuthenticate == null || wwwAuthenticate.isEmpty()) {
            return basic();
        }

        // Parse: "Bearer realm=\"...\",service=\"...\""
        Pattern schemePattern = Pattern.compile("^(\\w+)\\s+");
        Matcher schemeMatcher = schemePattern.matcher(wwwAuthenticate);
        String scheme = schemeMatcher.find() ? schemeMatcher.group(1) : "Basic";

        Pattern realmPattern = Pattern.compile("realm=\"([^\"]+)\"");
        Matcher realmMatcher = realmPattern.matcher(wwwAuthenticate);
        String realm = realmMatcher.find() ? realmMatcher.group(1) : null;

        Pattern servicePattern = Pattern.compile("service=\"([^\"]+)\"");
        Matcher serviceMatcher = servicePattern.matcher(wwwAuthenticate);
        String service = serviceMatcher.find() ? serviceMatcher.group(1) : null;

        return new AuthChallenge(scheme, realm, service);
    }

    static AuthChallenge basic() {
        return new AuthChallenge("Basic", null, null);
    }

    String getScheme() {
        return scheme;
    }

    String getRealm() {
        return realm;
    }

    String getService() {
        return service;
    }
}
