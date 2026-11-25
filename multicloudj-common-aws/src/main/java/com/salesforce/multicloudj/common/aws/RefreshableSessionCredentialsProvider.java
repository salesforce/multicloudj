package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.CredentialsRefresher;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.time.Instant;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

/**
 * An {@link AwsCredentialsProvider} that serves credentials from a {@link CredentialsRefresher}.
 *
 * <p>The AWS SDK resolves credentials on every API call and does not cache them for a plain {@link
 * AwsCredentialsProvider}, so this provider asks the refresher on every call. Caching, refresh
 * timing and failure handling are the refresher's responsibility.
 */
public class RefreshableSessionCredentialsProvider implements AwsCredentialsProvider {
  private final CredentialsRefresher refresher;

  public RefreshableSessionCredentialsProvider(CredentialsRefresher refresher) {
    this.refresher = refresher;
  }

  @Override
  public AwsCredentials resolveCredentials() {
    StsCredentials credentials = refresher.getCredentials();
    AwsSessionCredentials.Builder session =
        AwsSessionCredentials.builder()
            .accessKeyId(credentials.getAccessKeyId())
            .secretAccessKey(credentials.getAccessKeySecret())
            .sessionToken(credentials.getSecurityToken());
    Instant expiration = credentials.getExpiration();
    if (expiration != null) {
      session.expirationTime(expiration);
    }
    return session.build();
  }
}
