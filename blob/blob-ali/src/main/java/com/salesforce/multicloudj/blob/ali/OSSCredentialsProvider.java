package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.credentials.Credentials;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.credentials.StaticCredentialsProvider;
import com.salesforce.multicloudj.sts.client.StsClient;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

public class OSSCredentialsProvider {

  private static final int DEFAULT_DURATION_SECONDS = 3600;

  /** Returns an OSS SDK CredentialsProvider. */
  public static CredentialsProvider getCredentialsProvider(
      CredentialsOverrider overrider, String region) {
    if (overrider == null || overrider.getType() == null) {
      return null;
    }

    switch (overrider.getType()) {
      case SESSION:
        StsCredentials stsCredentials = overrider.getSessionCredentials();
        return new StaticCredentialsProvider(
            stsCredentials.getAccessKeyId(),
            stsCredentials.getAccessKeySecret(),
            stsCredentials.getSecurityToken());
      case ASSUME_ROLE:
        int duration = overrider.getDurationSeconds() != null
            ? overrider.getDurationSeconds() : DEFAULT_DURATION_SECONDS;
        return new AssumeRoleCredentialsProvider(
            region,
            overrider.getRole(),
            overrider.getSessionName(),
            duration);
      default:
        return null;
    }
  }

  /**
   * A {@link CredentialsProvider} that obtains temporary credentials by assuming an IAM role via
   * the multicloudj STS module. Credentials are cached and automatically refreshed before expiry.
   */
  static class AssumeRoleCredentialsProvider implements CredentialsProvider {

    private final String region;
    private final AssumedRoleRequest roleRequest;
    private final long refreshMarginMillis;

    private volatile StsClient stsClient;
    private volatile Credentials cachedCredentials;
    private volatile long expirationTime;

    AssumeRoleCredentialsProvider(String region, String role,
        String sessionName, int durationSeconds) {
      this.region = region;
      this.roleRequest = AssumedRoleRequest.newBuilder()
          .withRole(role)
          .withSessionName(sessionName != null
              ? sessionName
              : "multicloudj-" + System.currentTimeMillis())
          .withExpiration(durationSeconds)
          .build();
      // Refresh at 80% of the token lifetime
      this.refreshMarginMillis = (long) (durationSeconds * 1000L * 0.8);
    }

    @Override
    public Credentials getCredentials() {
      if (cachedCredentials == null || isExpiringSoon()) {
        refresh();
      }
      return cachedCredentials;
    }

    private synchronized void refresh() {
      if (cachedCredentials != null && !isExpiringSoon()) {
        return;
      }
      if (stsClient == null) {
        stsClient = StsClient.builder("ali")
            .withRegion(region)
            .build();
      }
      StsCredentials assumed = stsClient.getAssumeRoleCredentials(roleRequest);
      cachedCredentials = new Credentials(
          assumed.getAccessKeyId(),
          assumed.getAccessKeySecret(),
          assumed.getSecurityToken());
      expirationTime = System.currentTimeMillis() + refreshMarginMillis;
    }

    private boolean isExpiringSoon() {
      return System.currentTimeMillis() >= expirationTime;
    }
  }
}
