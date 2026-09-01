package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;

/**
 * Builds an Alibaba SMQ (MNS) {@link MNSClient} from a multicloudj endpoint, credentials, and an
 * optional proxy.
 *
 * <p>The SMQ endpoint is account-scoped ({@code https://<accountId>.mns.<region>.aliyuncs.com}), so
 * a non-null endpoint is required: the account id is not derivable from region and credentials
 * alone.
 */
public final class MnsClientUtil {

  private MnsClientUtil() {}

  /**
   * Builds an {@link MNSClient}.
   *
   * @param endpoint the account-scoped SMQ endpoint (required)
   * @param credentialsOverrider the SESSION credentials to use (required); the SMQ SDK requires a
   *     non-null access key, so credential-less construction is not supported
   * @param proxyEndpoint an optional HTTP proxy; may be {@code null}
   * @return a configured {@link MNSClient}
   * @throws InvalidArgumentException if {@code endpoint} is {@code null}, or if usable credentials
   *     cannot be resolved from {@code credentialsOverrider}
   */
  public static MNSClient buildMnsClient(
      URI endpoint, CredentialsOverrider credentialsOverrider, URI proxyEndpoint) {
    if (endpoint == null) {
      throw new InvalidArgumentException(
          "SMQ endpoint is required "
              + "(account-scoped host, e.g. https://<accountId>.mns.<region>.aliyuncs.com)");
    }

    String accountEndpoint = endpoint.toString();

    ClientConfiguration clientConfiguration = new ClientConfiguration();
    if (proxyEndpoint != null) {
      clientConfiguration.setProxyHost(proxyEndpoint.getHost());
      if (proxyEndpoint.getPort() > 0) {
        clientConfiguration.setProxyPort(proxyEndpoint.getPort());
      }
    }

    AlibabaCloudCredentialsProvider credentialsProvider =
        MnsCredentialsProvider.getCredentialsProvider(credentialsOverrider);
    if (credentialsProvider == null) {
      throw new InvalidArgumentException(
          "SMQ credentials are required; supply SESSION credentials via CredentialsOverrider");
    }

    CloudAccount cloudAccount =
        new CloudAccount(accountEndpoint, credentialsProvider, clientConfiguration);
    return cloudAccount.getMNSClient();
  }
}
