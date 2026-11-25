package com.salesforce.multicloudj.common.gcp;

import com.google.auth.RequestMetadataCallback;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.sts.model.CredentialsRefresher;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * {@link GoogleCredentials} that serve the security token of the credentials held by a {@link
 * CredentialsRefresher} as the OAuth2 bearer token.
 *
 * <p>The refresher owns all caching and timing. The Google auth library caches the access token it
 * last saw and only asks for a new one when that token's expiration is near, and it treats a token
 * without an expiration as valid forever, so its cache cannot follow the refresher's schedule.
 * These credentials therefore never serve request metadata from that cache. Every request-metadata
 * call asks the refresher for the current credentials, which is cheap while they are fresh.
 *
 * <p>The cached token returned by {@link #getAccessToken()} is only updated by {@link #refresh()}
 * and {@link #refreshIfExpired()}, so callers that read it directly must call {@link
 * #refreshIfExpired()} first. Here {@link #refreshIfExpired()} always re-reads the refresher.
 *
 * <p>Only the security token is used. The access key id and secret of the supplied credentials are
 * ignored. A supplier that returns an empty security token is rejected with an {@link
 * InvalidArgumentException}.
 *
 * <p>Failures from the refresher, such as an unavailable supplier, propagate unchanged from the
 * synchronous methods and are delivered through {@link RequestMetadataCallback#onFailure} by the
 * asynchronous one. The first call performs the first fetch, so the supplier is invoked by
 * whichever client first asks for request metadata. Some clients do this while they are being
 * built.
 *
 * <p>The library's copy methods that return the same instance ({@code createScoped}, {@code
 * createDelegated}, {@code createWithCustomRetryStrategy}) keep the refresher. The ones that would
 * build plain credentials from the cached token ({@code createWithQuotaProject}, {@code
 * toBuilder}) throw {@link UnSupportedOperationException} instead of silently dropping it.
 *
 * <p>Instances are thread-safe. Several instances may share one refresher and then share one
 * supplier.
 */
public class RefreshableSessionCredentials extends GoogleCredentials {

  private static final String AUTHORIZATION = "Authorization";
  private static final String BEARER_PREFIX = "Bearer ";

  private final CredentialsRefresher refresher;

  public RefreshableSessionCredentials(CredentialsRefresher refresher) {
    super();
    if (refresher == null) {
      throw new InvalidArgumentException("refresher must not be null");
    }
    this.refresher = refresher;
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) {
    return toRequestMetadata(currentToken());
  }

  /**
   * Delivers the metadata on the supplied executor when there is one, because a refresh may call a
   * supplier that blocks and callers such as gRPC must not be blocked.
   */
  @Override
  public void getRequestMetadata(
      URI uri, Executor executor, RequestMetadataCallback callback) {
    if (executor == null) {
      deliver(callback);
      return;
    }
    try {
      executor.execute(() -> deliver(callback));
    } catch (RuntimeException e) {
      callback.onFailure(e);
    }
  }

  /**
   * Forces the cached token to be replaced with the refresher's current credentials, because the
   * library's own freshness check cannot see the refresher's schedule.
   */
  @Override
  public void refreshIfExpired() throws IOException {
    refresh();
  }

  @Override
  public AccessToken refreshAccessToken() {
    StsCredentials credentials = currentCredentials();
    AccessToken.Builder token = AccessToken.newBuilder().setTokenValue(tokenOf(credentials));
    Instant expiration = credentials.getExpiration();
    if (expiration != null) {
      token.setExpirationTime(Date.from(expiration));
    }
    return token.build();
  }

  /**
   * Not supported: the library builds the copy from the cached token and returns plain {@link
   * GoogleCredentials}, which would silently stop asking the refresher.
   */
  @Override
  public GoogleCredentials createWithQuotaProject(String quotaProject) {
    throw copyNotSupported();
  }

  /** Not supported, for the same reason as {@link #createWithQuotaProject(String)}. */
  @Override
  public GoogleCredentials.Builder toBuilder() {
    throw copyNotSupported();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof RefreshableSessionCredentials
        && ((RefreshableSessionCredentials) other).refresher == refresher;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(refresher);
  }

  /** Omits the token, which the inherited implementation would print. */
  @Override
  public String toString() {
    return "RefreshableSessionCredentials";
  }

  private static UnSupportedOperationException copyNotSupported() {
    return new UnSupportedOperationException(
        "Credentials from a session credentials supplier cannot be copied because a copy would not"
            + " keep asking the credentials refresher");
  }

  private void deliver(RequestMetadataCallback callback) {
    Map<String, List<String>> metadata;
    try {
      metadata = toRequestMetadata(currentToken());
    } catch (RuntimeException e) {
      callback.onFailure(e);
      return;
    }
    callback.onSuccess(metadata);
  }

  private String currentToken() {
    return tokenOf(currentCredentials());
  }

  private StsCredentials currentCredentials() {
    return refresher.getCredentials();
  }

  private static String tokenOf(StsCredentials credentials) {
    String token = credentials.getSecurityToken();
    if (token == null || token.isBlank()) {
      throw new InvalidArgumentException(
          "Credentials supplied by a session credentials supplier must carry a security token");
    }
    return token;
  }

  private Map<String, List<String>> toRequestMetadata(String token) {
    Map<String, List<String>> metadata = new LinkedHashMap<>();
    metadata.put(AUTHORIZATION, List.of(BEARER_PREFIX + token));
    metadata.putAll(getAdditionalHeaders());
    return Collections.unmodifiableMap(metadata);
  }
}
