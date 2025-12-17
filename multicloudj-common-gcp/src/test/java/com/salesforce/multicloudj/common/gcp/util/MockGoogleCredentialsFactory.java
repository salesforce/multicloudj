package com.salesforce.multicloudj.common.gcp.util;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenProvider;

import java.io.IOException;
import java.util.Date;

/**
 * Factory for creating mock {@link GoogleCredentials} instances for playback-mode tests.
 * The credentials override {@code refreshAccessToken()} to return a static long-lived token,
 * preventing external calls during test playback.
 */
public final class MockGoogleCredentialsFactory {

    private MockGoogleCredentialsFactory() {
        // prevent instantiation from outside
    }

    /**
     * Creates mock credentials with a long-lived access token for testing.
     */
    public static GoogleCredentials createMockCredentials() {
        // 100 years in the future
        Date futureDate = new Date(System.currentTimeMillis() + 100L * 365 * 24 * 60 * 60 * 1000);
        AccessToken token = new AccessToken("mock-gcp-oauth2-token", futureDate);

        return new MockGoogleCredentialsWithIdToken(token);
    }

    private static class MockGoogleCredentialsWithIdToken extends GoogleCredentials implements IdTokenProvider {
        private final AccessToken staticToken;

        MockGoogleCredentialsWithIdToken(AccessToken token) {
            this.staticToken = token;
        }

        @Override
        public AccessToken refreshAccessToken() {
            // Always return the pre-generated token; no external call needed.
            return staticToken;
        }

        @Override
        public IdToken idTokenWithAudience(String targetAudience, java.util.List<IdTokenProvider.Option> options) throws IOException {
            // Return a mock ID token for testing
            // Create a JWT with the requested audience
            String mockJwt = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20iLCJzdWIiOiJtb2NrLXVzZXIiLCJhdWQiOiJtdWx0aWNsb3VkaiIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxMjM0NTY3ODkwfQ.mock-signature";
            return IdToken.create(mockJwt);
        }
    }
}

