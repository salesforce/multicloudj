package com.salesforce.multicloudj.common.aws;

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import java.time.Instant;
import java.util.function.Supplier;

public class RefreshingWebIdentityProvider implements AwsCredentialsProvider {
    private final Supplier<AwsCredentialsProvider> providerFactory;
    private volatile AwsCredentialsProvider currentDelegate;
    private volatile Instant expiration;

    public RefreshingWebIdentityProvider(Supplier<AwsCredentialsProvider> providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public AwsCredentials resolveCredentials() {
        // If we don't have a provider, or if the current one is likely expired
        // (Buffer of 5 minutes to be safe), we rebuild it.
        if (currentDelegate == null || Instant.now().isAfter(expiration.minusSeconds(300))) {
            synchronized (this) {
                if (currentDelegate == null || Instant.now().isAfter(expiration.minusSeconds(300))) {
                    currentDelegate = providerFactory.get();
                    AwsCredentials creds = currentDelegate.resolveCredentials();
                    // Extract expiration if available, otherwise default to 1 hour
                    expiration = creds.expirationTime().orElse(Instant.now().plusSeconds(3600));
                    return creds;
                }
            }
        }
        return currentDelegate.resolveCredentials();
    }
}