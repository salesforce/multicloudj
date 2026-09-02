package com.salesforce.multicloudj.pubsub.ali;

import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.StaticCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.StsCredentials;

/**
 * Maps a multicloudj {@link CredentialsOverrider} to an Alibaba credentials provider for the SMQ
 * (MNS) {@code CloudAccount}.
 *
 * <p>Only an <b>absent</b> override falls back to ambient credentials: this returns {@code null}
 * for a {@code null} overrider or {@code null} type, and the caller then resolves the Alibaba
 * default credential chain. Any explicit override that cannot be honored fails fast rather than
 * silently using ambient credentials:
 *
 * <ul>
 *   <li>{@code SESSION} with session credentials → a static session-credentials provider (the
 *       credential values are passed to the SDK, which evaluates them);
 *   <li>{@code SESSION} without session credentials → {@link InvalidArgumentException};
 *   <li>{@code ASSUME_ROLE} / {@code ASSUME_ROLE_WEB_IDENTITY} →
 *       {@link UnSupportedOperationException} (TODO: not yet implemented for SMQ).
 * </ul>
 */
public final class MnsCredentialsProvider {

  private MnsCredentialsProvider() {}

  /**
   * Returns an Alibaba credentials provider for the given overrider, or {@code null} only when the
   * override is absent (the caller then resolves the default credential chain).
   *
   * @param overrider the credentials override
   * @return a static session provider for a {@code SESSION} override with session credentials, or
   *     {@code null} when the override is absent
   * @throws InvalidArgumentException if a {@code SESSION} override carries no session credentials
   * @throws UnSupportedOperationException if the override type is not yet supported for SMQ
   */
  public static AlibabaCloudCredentialsProvider getCredentialsProvider(
      CredentialsOverrider overrider) {
    if (overrider == null || overrider.getType() == null) {
      return null;
    }

    switch (overrider.getType()) {
      case SESSION:
        StsCredentials sessionCredentials = overrider.getSessionCredentials();
        if (sessionCredentials == null) {
          throw new InvalidArgumentException(
              "SESSION credentials override requires session credentials");
        }
        return new StaticCredentialsProvider(
            new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(),
                sessionCredentials.getAccessKeySecret(),
                sessionCredentials.getSecurityToken()));
      case ASSUME_ROLE:
      case ASSUME_ROLE_WEB_IDENTITY:
        // TODO: implement ASSUME_ROLE / ASSUME_ROLE_WEB_IDENTITY for SMQ.
        throw new UnSupportedOperationException(
            "SMQ credentials override type is not supported yet: " + overrider.getType());
      default:
        throw new UnSupportedOperationException(
            "SMQ credentials override type is not supported: " + overrider.getType());
    }
  }
}
