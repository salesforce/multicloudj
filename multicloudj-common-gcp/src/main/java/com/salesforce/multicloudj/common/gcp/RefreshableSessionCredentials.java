package com.salesforce.multicloudj.common.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.function.Supplier;

/**
 * Session credentials that renew themselves through a caller-supplied callback rather than holding
 * a single access token for the lifetime of the client.
 *
 * <p>Google's auth library only renews a token once that token reports itself as expired, and it
 * treats a token carrying no expiration time as permanently fresh. Every token produced here
 * therefore carries an expiration: the one declared by {@link StsCredentials#getExpiration()} when
 * the callback supplies it, and {@link #DEFAULT_TOKEN_LIFETIME} measured from the moment the token
 * is produced when it does not. The library renews a few minutes ahead of the expiration it is
 * given, so the effective renewal cadence is slightly shorter than the declared lifetime.
 *
 * <p>That same margin means a callback returning credentials which are already expired, or which
 * expire within the margin, would be asked for a renewal on every request. The token from the last
 * callback invocation is therefore reused for {@link #MINIMUM_REFRESH_INTERVAL} before the callback
 * is invoked again. The reused token keeps its original expiration, so the auth library still sees
 * the credentials as expired; the bound limits how hard the callback is driven, it does not
 * disguise credentials that have lapsed.
 *
 * <p>{@code OAuth2Credentials} serialises refreshes on its own lock and publishes the outcome to
 * every waiting caller, so this class adds no locking of its own and the callback is invoked once
 * per renewal however many threads are in flight. The callback runs on request threads and must
 * itself be thread-safe.
 */
final class RefreshableSessionCredentials extends GoogleCredentials {

  private static final long serialVersionUID = 1L;

  /**
   * Lifetime applied to credentials that declare no expiration of their own. It stays comfortably
   * above the auth library's refresh margin so that renewal happens on a predictable cadence rather
   * than on every request.
   */
  static final Duration DEFAULT_TOKEN_LIFETIME = Duration.ofMinutes(15);

  /** The shortest interval permitted between two callback invocations. */
  static final Duration MINIMUM_REFRESH_INTERVAL = Duration.ofSeconds(1);

  private final Supplier<StsCredentials> sessionCredentialsSupplier;

  private volatile LastRefresh lastRefresh;

  RefreshableSessionCredentials(Supplier<StsCredentials> sessionCredentialsSupplier) {
    this.sessionCredentialsSupplier = sessionCredentialsSupplier;
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    Instant now = Instant.now();
    LastRefresh previous = lastRefresh;
    if (previous != null
        && Duration.between(previous.suppliedAt, now).compareTo(MINIMUM_REFRESH_INTERVAL) < 0) {
      return previous.token;
    }

    StsCredentials credentials = sessionCredentialsSupplier.get();
    if (credentials == null) {
      throw new IOException("Session credentials supplier returned no credentials");
    }
    String securityToken = credentials.getSecurityToken();
    if (securityToken == null || securityToken.isBlank()) {
      throw new IOException("Session credentials supplier returned no security token");
    }
    Instant expiration = credentials.getExpiration();
    if (expiration == null) {
      expiration = now.plus(DEFAULT_TOKEN_LIFETIME);
    }
    AccessToken token =
        AccessToken.newBuilder()
            .setTokenValue(securityToken)
            .setExpirationTime(Date.from(expiration))
            .build();
    lastRefresh = new LastRefresh(token, now);
    return token;
  }

  /** What the last callback invocation produced, and when it was invoked. */
  private static final class LastRefresh {
    private final AccessToken token;
    private final Instant suppliedAt;

    LastRefresh(AccessToken token, Instant suppliedAt) {
      this.token = token;
      this.suppliedAt = suppliedAt;
    }
  }
}
