package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class CredentialsProvider {
    public static AwsCredentialsProvider getCredentialsProvider(CredentialsOverrider overrider, Region region) {
        if (overrider == null || overrider.getType() == null) {
            return null;
        }
        switch (overrider.getType()) {
            case SESSION: {
                StsCredentials stsCredentials = overrider.getSessionCredentials();
                AwsSessionCredentials sessionCredentials = AwsSessionCredentials.create(
                        stsCredentials.getAccessKeyId(),
                        stsCredentials.getAccessKeySecret(),
                        stsCredentials.getSecurityToken()
                );
                return StaticCredentialsProvider.create(sessionCredentials);
            }
            case ASSUME_ROLE: {
                String assumeRole = overrider.getRole();
                String sessionName = overrider.getSessionName() != null
                        ? overrider.getSessionName() : "multicloudj-" + System.currentTimeMillis();
                StsClient stsClient = StsClient.builder().region(region).build();
                AssumeRoleRequest.Builder assumeRoleRequestBuilder = AssumeRoleRequest.builder()
                        .roleArn(assumeRole)
                        .roleSessionName(sessionName);
                if (overrider.getDurationSeconds() != null) {
                    assumeRoleRequestBuilder.durationSeconds(overrider.getDurationSeconds());
                }

                return StsAssumeRoleCredentialsProvider.builder()
                        .stsClient(stsClient)
                        .refreshRequest(assumeRoleRequestBuilder.build())
                        .build();
            }
            case ASSUME_ROLE_WEB_IDENTITY: {
                // AWS SDK has a bug which doesn't refresh the web identity token after the initialization.
                // RefreshingWebIdentityProvider is a workaround for this which forces the token refresh.
                // details: https://github.com/aws/aws-sdk-java-v2/issues/5709
                if (overrider.getWebIdentityTokenSupplier() == null) {
                    throw new IllegalArgumentException("webIdentityTokenSupplier must be provided for ASSUME_ROLE_WEB_IDENTITY");
                }

                // We wrap the provider creation in a factory lambda
                return new RefreshingWebIdentityProvider(() -> {
                    String assumeRole = overrider.getRole();
                    String sessionName = overrider.getSessionName() != null
                            ? overrider.getSessionName() : "multicloudj-web-identity-" + System.currentTimeMillis();

                    // 1. Always use Anonymous for the STS client to avoid recursion
                    StsClient stsClient = StsClient.builder()
                            .credentialsProvider(AnonymousCredentialsProvider.create())
                            .region(region)
                            .build();

                    // 2. Build the actual provider
                    return StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
                            .stsClient(stsClient)
                            .refreshRequest(builder -> {
                                builder.roleArn(assumeRole)
                                        .webIdentityToken(overrider.getWebIdentityTokenSupplier().get())
                                        .roleSessionName(sessionName);

                                if (overrider.getDurationSeconds() != null) {
                                    builder.durationSeconds(overrider.getDurationSeconds());
                                }
                            })
                            .build();
                });
            }
        }
        return null;
    }

}
