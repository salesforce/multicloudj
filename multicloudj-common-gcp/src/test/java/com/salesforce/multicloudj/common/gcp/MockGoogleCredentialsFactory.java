package com.salesforce.multicloudj.common.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

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

        return new GoogleCredentials() {
            /** cached static token – never expires within the 100-year window */
            private final AccessToken staticToken = token;

            @Override
            public AccessToken refreshAccessToken() {
                // Always return the pre-generated token; no external call needed.
                return staticToken;
            }
        };
    }
}

