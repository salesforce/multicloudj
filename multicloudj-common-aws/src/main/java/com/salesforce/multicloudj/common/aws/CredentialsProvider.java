package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class CredentialsProvider {

  private static final Logger log = LoggerFactory.getLogger(CredentialsProvider.class);

  public static AwsCredentialsProvider getCredentialsProvider(
      CredentialsOverrider overrider, Region region) {
    if (overrider == null || overrider.getType() == null) {
      return null;
    }
    AwsCredentialsProvider provider = resolveCredentialsProvider(overrider, region);
    String message =
        "MultiCloudJ AWS credentials provider selected: credentialsType={}, providerClass={}";
    String providerClass = provider == null ? "none" : provider.getClass().getName();
    // Session credentials are the only type whose provider class tells an operator something they
    // cannot infer, namely whether an expired-credentials report came from a client that renews
    // credentials or one that holds them for its whole lifetime. The rest is debug-level noise.
    if (overrider.getType() == CredentialsType.SESSION) {
      log.info(message, overrider.getType(), providerClass);
    } else {
      log.debug(message, overrider.getType(), providerClass);
    }
    return provider;
  }

  private static AwsCredentialsProvider resolveCredentialsProvider(
      CredentialsOverrider overrider, Region region) {
    switch (overrider.getType()) {
      case SESSION:
        {
          if (overrider.getSessionCredentialsSupplier() != null) {
            return new RefreshingSessionCredentialsProvider(
                overrider.getSessionCredentialsSupplier());
          }
          StsCredentials stsCredentials = overrider.getSessionCredentials();
          if (StringUtils.isBlank(stsCredentials.getSecurityToken())) {
            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(
                    stsCredentials.getAccessKeyId(),
                    stsCredentials.getAccessKeySecret())
            );
          }
          AwsSessionCredentials sessionCredentials =
              AwsSessionCredentials.create(
                  stsCredentials.getAccessKeyId(),
                  stsCredentials.getAccessKeySecret(),
                  stsCredentials.getSecurityToken());
          return StaticCredentialsProvider.create(sessionCredentials);
        }
      case ASSUME_ROLE:
        {
          String assumeRole = overrider.getRole();
          String sessionName =
              overrider.getSessionName() != null
                  ? overrider.getSessionName()
                  : "multicloudj-" + System.currentTimeMillis();
          StsClient stsClient = StsClient.builder().region(region).build();
          AssumeRoleRequest.Builder assumeRoleRequestBuilder =
              AssumeRoleRequest.builder().roleArn(assumeRole).roleSessionName(sessionName);
          if (overrider.getDurationSeconds() != null) {
            assumeRoleRequestBuilder.durationSeconds(overrider.getDurationSeconds());
          }

          return StsAssumeRoleCredentialsProvider.builder()
              .stsClient(stsClient)
              .refreshRequest(assumeRoleRequestBuilder.build())
              .build();
        }
      case ASSUME_ROLE_WEB_IDENTITY:
        {
          // AWS SDK has a bug which doesn't refresh the web identity token after the
          // initialization.
          // RefreshingWebIdentityProvider is a workaround for this which forces the token refresh.
          // details: https://github.com/aws/aws-sdk-java-v2/issues/5709
          if (overrider.getWebIdentityTokenSupplier() == null) {
            throw new IllegalArgumentException(
                "webIdentityTokenSupplier must be provided for ASSUME_ROLE_WEB_IDENTITY");
          }

          // We wrap the provider creation in a factory lambda
          return new RefreshingWebIdentityProvider(
              () -> {
                String assumeRole = overrider.getRole();
                String sessionName =
                    overrider.getSessionName() != null
                        ? overrider.getSessionName()
                        : "multicloudj-web-identity-" + System.currentTimeMillis();

                // 1. Always use Anonymous for the STS client to avoid recursion
                StsClient stsClient =
                    StsClient.builder()
                        .credentialsProvider(AnonymousCredentialsProvider.create())
                        .region(region)
                        .build();

                // 2. Build the actual provider
                return StsAssumeRoleWithWebIdentityCredentialsProvider.builder()
                    .stsClient(stsClient)
                    .refreshRequest(
                        builder -> {
                          builder
                              .roleArn(assumeRole)
                              .webIdentityToken(overrider.getWebIdentityTokenSupplier().get())
                              .roleSessionName(sessionName);

                          if (overrider.getDurationSeconds() != null) {
                            builder.durationSeconds(overrider.getDurationSeconds());
                          }
                        })
                    .build();
              });
        }
      default:
        return null;
    }
  }
}
