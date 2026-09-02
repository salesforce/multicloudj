package com.salesforce.multicloudj.pubsub.ali;

import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

/**
 * Maps a multicloudj {@link CredentialsOverrider} to an Alibaba credentials provider suitable for
 * constructing an SMQ (MNS) {@code CloudAccount}.
 *
 * <p>Only {@code SESSION} (access key id/secret + security token) is wired today. {@code
 * ASSUME_ROLE} and {@code ASSUME_ROLE_WEB_IDENTITY} are deferred (they depend on the broader
 * Alibaba STS integration) and resolve to {@code null} here.
 */
public final class MnsCredentialsProvider {

  private MnsCredentialsProvider() {}

  /**
   * Returns an Alibaba credentials provider for the given overrider.
   *
   * @return {@code null} if the overrider is absent or its type is not supported (so the caller can
   *     reject it); never returns a provider built from incomplete credentials
   * @throws InvalidArgumentException if the type is {@code SESSION} but the session credentials are
   *     missing or have a blank access key id, access key secret, or security token
   */
  public static AlibabaCloudCredentialsProvider getCredentialsProvider(
      CredentialsOverrider overrider) {
    if (overrider == null || overrider.getType() == null) {
      return null;
    }

    switch (overrider.getType()) {
      case SESSION:
        StsCredentials sessionCredentials = overrider.getSessionCredentials();
        if (sessionCredentials == null
            || isBlank(sessionCredentials.getAccessKeyId())
            || isBlank(sessionCredentials.getAccessKeySecret())
            || isBlank(sessionCredentials.getSecurityToken())) {
          throw new InvalidArgumentException(
              "SESSION credentials are incomplete: accessKeyId, accessKeySecret, and "
                  + "securityToken are all required");
        }
        return new StaticCredentialsProvider(
            new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getAccessKeySecret(),
                sessionCredentials.getSecurityToken()));
      default:
        // ASSUME_ROLE / ASSUME_ROLE_WEB_IDENTITY are not yet wired for SMQ.
        return null;
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
