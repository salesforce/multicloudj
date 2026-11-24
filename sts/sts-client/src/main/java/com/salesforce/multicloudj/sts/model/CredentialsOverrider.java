package com.salesforce.multicloudj.sts.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import java.time.Duration;
import java.util.function.Supplier;
import lombok.Getter;

/**
 * The CredentialsOverrider is used when the service wants to override the default
 * credentialsOverrider in the given environment. The default credentialsOverrider are used for the
 * majority of the use-cases and are overridden in few cases including but not limited to: 1. When
 * the service wants to assume the role from another account 2. Service wants to supply session
 * credentialsOverrider for testing purposes etc. There can be more use-cases in future but for now
 * we cover the two listed above. If the service supplies both, the session credentialsOverrider and
 * the details for assume role, the session credentialsOverrider takes precedence over the assume
 * role.
 *
 * <p>{@link CredentialsType#REFRESHABLE_SESSION} is for credentials that only the caller can
 * obtain. The caller supplies a {@link Supplier} that returns session credentials, and the SDK
 * calls it again when the credentials it last returned need refreshing, so long-lived clients keep
 * working after those credentials expire. Refreshing is lazy and driven by use: a client that is
 * idle does not call the supplier. Credentials that carry an expiration ({@link
 * StsCredentials#getExpiration()}) are refreshed after the smaller of the refresh interval and 80%
 * of their remaining lifetime. Credentials without one are refreshed after the refresh interval.
 */
@Getter
public class CredentialsOverrider {
  protected CredentialsType type;
  protected StsCredentials sessionCredentials;
  protected String role;
  protected Integer durationSeconds;
  protected String sessionName;
  protected Supplier<String> webIdentityTokenSupplier;
  protected Supplier<StsCredentials> sessionCredentialsSupplier;
  protected Duration refreshInterval;
  protected CredentialsRefresher credentialsRefresher;

  public CredentialsOverrider(Builder builder) {
    this.type = builder.type;
    this.sessionCredentials = builder.sessionCredentials;
    this.role = builder.role;
    this.durationSeconds = builder.durationSeconds;
    this.webIdentityTokenSupplier = builder.webIdentityTokenSupplier;
    this.sessionName = builder.sessionName;
    this.sessionCredentialsSupplier = builder.sessionCredentialsSupplier;
    this.refreshInterval = builder.refreshInterval;
    if (builder.type == CredentialsType.REFRESHABLE_SESSION) {
      this.credentialsRefresher =
          new CredentialsRefresher(builder.sessionCredentialsSupplier, builder.refreshInterval);
    }
  }

  public static class Builder {
    private final CredentialsType type;
    private StsCredentials sessionCredentials;
    private String role;
    private Integer durationSeconds;
    protected String sessionName;
    protected Supplier<String> webIdentityTokenSupplier;
    private Supplier<StsCredentials> sessionCredentialsSupplier;
    private Duration refreshInterval;

    public Builder(CredentialsType type) {
      this.type = type;
    }

    public Builder withSessionCredentials(StsCredentials sessionCredentials) {
      this.sessionCredentials = sessionCredentials;
      return this;
    }

    public Builder withRole(String role) {
      this.role = role;
      return this;
    }

    public Builder withDurationSeconds(Integer durationSeconds) {
      this.durationSeconds = durationSeconds;
      return this;
    }

    public Builder withWebIdentityTokenSupplier(Supplier<String> tokenSupplier) {
      this.webIdentityTokenSupplier = tokenSupplier;
      return this;
    }

    public Builder withSessionName(String sessionName) {
      this.sessionName = sessionName;
      return this;
    }

    /**
     * Sets the supplier that returns fresh session credentials. Only valid for {@link
     * CredentialsType#REFRESHABLE_SESSION}.
     *
     * <p>The returned credentials must carry a non-blank access key id and access key secret, even
     * for clouds that authenticate with the security token alone. A result without them counts as
     * a failed fetch, and the previous credentials stay in use. A supplier that knows when the
     * credentials expire should set {@link StsCredentials#getExpiration()}.
     */
    public Builder withSessionCredentialsSupplier(
        Supplier<StsCredentials> sessionCredentialsSupplier) {
      this.sessionCredentialsSupplier = sessionCredentialsSupplier;
      return this;
    }

    /**
     * Sets the longest time the SDK keeps using the credentials the supplier returned before it
     * calls the supplier again. The call happens on the first use after that time, and sooner when
     * the credentials carry an expiration: they are refreshed after 80% of their remaining
     * lifetime if that is shorter. Only valid for {@link CredentialsType#REFRESHABLE_SESSION}.
     * {@link #build()} rejects an interval too large for the SDK's time arithmetic.
     */
    public Builder withRefreshInterval(Duration refreshInterval) {
      this.refreshInterval = refreshInterval;
      return this;
    }

    public CredentialsOverrider build() {
      validate();
      return new CredentialsOverrider(this);
    }

    private void validate() {
      if (type == CredentialsType.REFRESHABLE_SESSION) {
        if (sessionCredentialsSupplier == null) {
          throw new InvalidArgumentException(
              "sessionCredentialsSupplier must be provided for REFRESHABLE_SESSION");
        }
        if (refreshInterval == null || refreshInterval.isZero() || refreshInterval.isNegative()) {
          throw new InvalidArgumentException(
              "refreshInterval must be provided and positive for REFRESHABLE_SESSION");
        }
        if (sessionCredentials != null) {
          throw new InvalidArgumentException(
              "static session credentials cannot be combined with REFRESHABLE_SESSION");
        }
      } else if (sessionCredentialsSupplier != null || refreshInterval != null) {
        throw new InvalidArgumentException(
            "sessionCredentialsSupplier and refreshInterval are only valid for"
                + " REFRESHABLE_SESSION");
      }
    }
  }
}
