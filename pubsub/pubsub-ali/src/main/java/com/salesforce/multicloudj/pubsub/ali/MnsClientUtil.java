package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudAccount;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.aliyuncs.auth.AlibabaCloudCredentialsProvider;
import com.aliyuncs.auth.DefaultCredentialsProvider;
import com.aliyuncs.exceptions.ClientException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
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
   * @param credentialsOverrider the credentials override, or {@code null} to use the Alibaba
   *     default credential chain; an explicit override that cannot be honored is rejected rather
   *     than silently falling back to ambient credentials
   * @param proxyEndpoint an optional HTTP proxy ({@code http://host:port}); may be {@code null}
   * @return a configured {@link MNSClient}
   * @throws InvalidArgumentException if the endpoint or proxy is missing/malformed, or a {@code
   *     SESSION} override carries no session credentials
   * @throws UnSupportedOperationException if the credentials override type is not yet supported
   *     ({@code ASSUME_ROLE} / {@code ASSUME_ROLE_WEB_IDENTITY})
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
      // Absent override: resolve the Alibaba default credential chain (environment variables,
      // system properties, OIDC/RRSA, credentials files, ECS/ECI instance roles).
      credentialsProvider = defaultCredentialsProvider();
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
      // The SMQ ClientConfiguration applies an HTTP host:port proxy only: it has no proxy-scheme
      // setter and ignores a proxy whose port is not positive, so validateProxy requires an http
      // scheme with an explicit port and rejects the components the SDK cannot carry.
      clientConfiguration.setProxyHost(proxyEndpoint.getHost());
      clientConfiguration.setProxyPort(proxyEndpoint.getPort());
    }
    return clientConfiguration;
  }

  private static AlibabaCloudCredentialsProvider defaultCredentialsProvider() {
    try {
      // The DefaultCredentialsProvider ctor does no credential I/O; resolution happens lazily in
      // getCredentials(). Its only constructor throw is for an empty ALIBABA_CLOUD_ECS_METADATA.
      return new DefaultCredentialsProvider();
    } catch (ClientException e) {
      throw new InvalidArgumentException(
          "Invalid Alibaba credentials configuration in the environment "
              + "(check ALIBABA_CLOUD_ECS_METADATA)",
          e);
    }
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
    if (!isHttpOrHttps(endpoint.getScheme())) {
      throw new InvalidArgumentException("SMQ endpoint scheme must be http or https: " + endpoint);
    }
    if (endpoint.getPort() != -1 && !isValidPort(endpoint.getPort())) {
      throw new InvalidArgumentException(
          "SMQ endpoint port must be in the range 1-65535: " + endpoint);
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
    if (!isValidPort(proxyEndpoint.getPort())) {
      throw new InvalidArgumentException(
          "Proxy endpoint must include an explicit port in the range 1-65535: " + proxyEndpoint);
    }
    if (!"http".equals(lower(proxyEndpoint.getScheme()))) {
      throw new InvalidArgumentException(
          "Proxy endpoint scheme must be http (an HTTP host:port proxy): " + proxyEndpoint);
    }
    if (proxyEndpoint.getUserInfo() != null
        || proxyEndpoint.getQuery() != null
        || proxyEndpoint.getFragment() != null
        || !isRootPath(proxyEndpoint.getPath())) {
      throw new InvalidArgumentException(
          "Proxy endpoint must be host:port only, with no user-info, path, query, or fragment: "
              + proxyEndpoint);
    }
  }

  private static boolean isHttpOrHttps(String scheme) {
    String lower = lower(scheme);
    return "http".equals(lower) || "https".equals(lower);
  }

  private static String lower(String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }

  private static boolean isValidPort(int port) {
    return port >= 1 && port <= 65535;
  }

  private static boolean isRootPath(String path) {
    return path == null || path.isEmpty() || "/".equals(path);
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
