package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import java.util.Locale;

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
   * @throws InvalidArgumentException if the endpoint is missing or malformed, the proxy is
   *     malformed, or usable credentials cannot be resolved from {@code credentialsOverrider}
   */
  public static MNSClient buildMnsClient(
      URI endpoint, CredentialsOverrider credentialsOverrider, URI proxyEndpoint) {
    return buildCloudAccount(endpoint, credentialsOverrider, proxyEndpoint).getMNSClient();
  }

  /**
   * Builds the {@link CloudAccount} used by {@link #buildMnsClient}; package-private so unit tests
   * can assert the endpoint and credential wiring directly.
   */
  static CloudAccount buildCloudAccount(
      URI endpoint, CredentialsOverrider credentialsOverrider, URI proxyEndpoint) {
    validateEndpoint(endpoint);

    ClientConfiguration clientConfiguration = buildClientConfiguration(proxyEndpoint);

    AlibabaCloudCredentialsProvider credentialsProvider =
        MnsCredentialsProvider.getCredentialsProvider(credentialsOverrider);
    if (credentialsProvider == null) {
      throw new InvalidArgumentException(
          "SMQ credentials are required; supply SESSION credentials via CredentialsOverrider");
    }

    return new CloudAccount(endpoint.toString(), credentialsProvider, clientConfiguration);
  }

  /**
   * Builds the SDK client configuration used by {@link #buildCloudAccount}; package-private so unit
   * tests can assert the proxy wiring directly.
   */
  static ClientConfiguration buildClientConfiguration(URI proxyEndpoint) {
    ClientConfiguration clientConfiguration = new ClientConfiguration();
    if (proxyEndpoint != null) {
      validateProxy(proxyEndpoint);
      // Only the proxy host and port are applied. The SMQ ClientConfiguration exposes host/port
      // (plus optional proxy credentials) but has no proxy-scheme setter, so the proxy is used as
      // an HTTP host:port endpoint and the URI scheme, user-info, path, query, and fragment are
      // not applied. A blank host is rejected above; any other components are accepted but ignored.
      clientConfiguration.setProxyHost(proxyEndpoint.getHost());
      if (proxyEndpoint.getPort() > 0) {
        clientConfiguration.setProxyPort(proxyEndpoint.getPort());
      }
    }
    return clientConfiguration;
  }

  private static void validateEndpoint(URI endpoint) {
    if (endpoint == null) {
      throw new InvalidArgumentException(
          "SMQ endpoint is required "
              + "(account-scoped host, e.g. https://<accountId>.mns.<region>.aliyuncs.com)");
    }
    if (!endpoint.isAbsolute() || isBlank(endpoint.getHost())) {
      throw new InvalidArgumentException(
          "SMQ endpoint must be an absolute URL with a host: " + endpoint);
    }
    if (!isHttpScheme(endpoint.getScheme())) {
      throw new InvalidArgumentException("SMQ endpoint scheme must be http or https: " + endpoint);
    }
    if (endpoint.getUserInfo() != null
        || endpoint.getQuery() != null
        || endpoint.getFragment() != null
        || !isRootPath(endpoint.getPath())) {
      throw new InvalidArgumentException(
          "SMQ endpoint must be an account host with no user-info, path, query, or fragment: "
              + endpoint);
    }
  }

  private static void validateProxy(URI proxyEndpoint) {
    if (isBlank(proxyEndpoint.getHost())) {
      throw new InvalidArgumentException("Proxy endpoint must have a host: " + proxyEndpoint);
    }
    if (proxyEndpoint.getScheme() != null && !isHttpScheme(proxyEndpoint.getScheme())) {
      throw new InvalidArgumentException(
          "Proxy endpoint scheme must be http or https: " + proxyEndpoint);
    }
  }

  private static boolean isHttpScheme(String scheme) {
    if (scheme == null) {
      return false;
    }
    String lower = scheme.toLowerCase(Locale.ROOT);
    return "http".equals(lower) || "https".equals(lower);
  }

  private static boolean isRootPath(String path) {
    return path == null || path.isEmpty() || "/".equals(path);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
