package com.salesforce.multicloudj.sts.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;

import java.util.Date;

/**
 * Factory for creating mock {@link GoogleCredentials} instances that can be used in
 * playback-mode tests where no real Application Default Credentials (ADC) exist.
 */
public final class MockGoogleCredentialsFactory {

    private MockGoogleCredentialsFactory() {
        // prevent instantiation from outside
    }

    /**
     * Returns a simple {@link GoogleCredentials} object backed by a fixed, non-expiring
     * {@link AccessToken}.
     */
    public static GoogleCredentials createMockCredentials() {
        // 100 years in the future
        Date futureDate = new Date(System.currentTimeMillis() + 100L * 365 * 24 * 60 * 60 * 1000);
        AccessToken token = new AccessToken("mock-gcp-oauth2-token", futureDate);

        /*
         * GoogleCredentials.create(AccessToken) returns an immutable credential that cannot
         * refresh – calling refreshAccessToken() throws an IllegalStateException. The client
         * libraries used by GcpStsIT will call refreshAccessToken() even when we have already
         * supplied a long-lived token, so here we provide a small subclass that simply returns
         * the same static AccessToken every time it’s asked to refresh.
         */
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