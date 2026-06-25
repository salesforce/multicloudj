package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.BaseClientBuilder;
import com.aliyun.sdk.service.oss2.credentials.CredentialsProvider;
import com.aliyun.sdk.service.oss2.retry.Retryer;
import com.aliyun.sdk.service.oss2.transport.HttpClient;
import com.salesforce.multicloudj.blob.driver.BlobStoreBuilder;
import com.salesforce.multicloudj.common.retries.RetryConfig;
import java.time.Duration;

/**
 * Builds and configures Alibaba OSS SDK clients (sync {@code OSSClient} and async
 * {@code OSSAsyncClient}) from the MultiCloudJ builder configuration.
 *
 * <p>The sync and async clients are constructed from distinct SDK builder types, but both implement
 * the SDK's {@link BaseClientBuilder} contract and map the same MultiCloudJ settings (region,
 * credentials, endpoint, proxy, retry, timeouts, connection pool) the same way. This factory
 * centralizes that mapping so the two code paths cannot drift. Callers supply the SDK builder, the
 * resolved credentials and retryer, and a {@link HttpClientFactory} that knows how to build the
 * transport client appropriate for that client kind (the only sync-vs-async difference).
 */
public final class OssClientFactory {

  private OssClientFactory() {}

  /**
   * Builds the transport {@link HttpClient} for a specific client kind (sync or async) from the
   * resolved connection options. This is the only part of client construction that differs between
   * the sync and async paths, so it is supplied by the caller as a small strategy.
   */
  @FunctionalInterface
  public interface HttpClientFactory {
    HttpClient create(String proxyHost, Duration readWriteTimeout,
        Integer maxConnections, Duration idleConnectionTimeout);
  }

  /**
   * Resolves the value for the Ali SDK's single {@code readWriteTimeout} setting from the two
   * MultiCloudJ inputs that map onto it. {@code RetryConfig.attemptTimeout} (the more specific
   * per-attempt deadline) takes precedence over the transport-level {@code socketTimeout}; if
   * neither is set, returns {@code null} so the SDK default is left in place.
   */
  public static Duration resolveReadWriteTimeout(RetryConfig retryConfig, Duration socketTimeout) {
    if (retryConfig != null && retryConfig.getAttemptTimeout() != null) {
      return Duration.ofMillis(retryConfig.getAttemptTimeout());
    }
    return socketTimeout;
  }

  /**
   * Applies the shared MultiCloudJ → OSS SDK configuration to {@code clientBuilder}: region,
   * credentials, optional endpoint/proxy/retryer, and the read-write timeout. Connection-pool size
   * and idle-connection timeout are only settable via {@code HttpClientOptions}, not on the client
   * builder, so when the caller sets either we build an explicit transport client from those
   * options (carrying proxyHost + readWriteTimeout forward so nothing the builder would otherwise
   * set is lost); when neither is set we leave the SDK to construct its own default client and set
   * readWriteTimeout directly.
   *
   * @param clientBuilder the SDK sync or async client builder to configure
   * @param mcjBuilder the MultiCloudJ builder carrying the user configuration
   * @param creds the resolved OSS credentials provider (must be non-null)
   * @param retryer the resolved OSS retryer, or {@code null} for none
   * @param httpClientFactory builds the transport client appropriate for this client kind
   */
  public static <B extends BaseClientBuilder<B, T>, T> void configure(
      B clientBuilder,
      BlobStoreBuilder<?> mcjBuilder,
      CredentialsProvider creds,
      Retryer retryer,
      HttpClientFactory httpClientFactory) {
    clientBuilder
        .region(mcjBuilder.getRegion())
        .credentialsProvider(creds);

    if (mcjBuilder.getEndpoint() != null) {
      clientBuilder.endpoint(mcjBuilder.getEndpoint().toString());
    }
    String proxyHost = proxyHost(mcjBuilder);
    if (proxyHost != null) {
      clientBuilder.proxyHost(proxyHost);
    }
    if (retryer != null) {
      clientBuilder.retryer(retryer);
    }

    // socketTimeout and RetryConfig.attemptTimeout both map to the Ali SDK's single
    // readWriteTimeout setting. When both are set, attemptTimeout (the more specific per-attempt
    // deadline) takes precedence over the transport-level socketTimeout.
    Duration readWriteTimeout =
        resolveReadWriteTimeout(mcjBuilder.getRetryConfig(), mcjBuilder.getSocketTimeout());

    // Connection-pool size and idle-connection timeout are only settable via HttpClientOptions,
    // not on the OSS client builder. When the caller sets either, build an explicit transport
    // client from those options (carrying proxyHost + readWriteTimeout forward so nothing the
    // builder would otherwise set is lost). When neither is set, leave the SDK to construct its
    // own default client and set readWriteTimeout directly, preserving the prior behavior.
    if (mcjBuilder.getMaxConnections() != null || mcjBuilder.getIdleConnectionTimeout() != null) {
      clientBuilder.httpClient(
          httpClientFactory.create(
              proxyHost,
              readWriteTimeout,
              mcjBuilder.getMaxConnections(),
              mcjBuilder.getIdleConnectionTimeout()));
    } else if (readWriteTimeout != null) {
      clientBuilder.readWriteTimeout(readWriteTimeout);
    }
  }

  /** Formats the proxy endpoint as {@code host:port}, or {@code null} when no proxy is set. */
  private static String proxyHost(BlobStoreBuilder<?> builder) {
    return builder.getProxyEndpoint() != null
        ? builder.getProxyEndpoint().getHost() + ":" + builder.getProxyEndpoint().getPort()
        : null;
  }
}
