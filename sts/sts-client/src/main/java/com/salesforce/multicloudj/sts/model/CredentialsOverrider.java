package com.salesforce.multicloudj.sts.model;

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
 * <p>Session credentials can be supplied either by value through {@link
 * Builder#withSessionCredentials} or by callback through {@link
 * Builder#withSessionCredentialsSupplier}. Clients hold a single underlying cloud connection for
 * their entire lifetime, so credentials supplied by value are fixed for that lifetime and any
 * service call made after they expire fails. Long-lived clients should supply a callback instead so
 * that expiring credentials can be renewed in place. When both are set, the callback wins.
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

  public CredentialsOverrider(Builder builder) {
    this.type = builder.type;
    this.sessionCredentials = builder.sessionCredentials;
    this.role = builder.role;
    this.durationSeconds = builder.durationSeconds;
    this.webIdentityTokenSupplier = builder.webIdentityTokenSupplier;
    this.sessionName = builder.sessionName;
    this.sessionCredentialsSupplier = builder.sessionCredentialsSupplier;
  }

  public static class Builder {
    private final CredentialsType type;
    private StsCredentials sessionCredentials;
    private String role;
    private Integer durationSeconds;
    protected String sessionName;
    protected Supplier<String> webIdentityTokenSupplier;
    protected Supplier<StsCredentials> sessionCredentialsSupplier;

    public Builder(CredentialsType type) {
      this.type = type;
    }

    public Builder withSessionCredentials(StsCredentials sessionCredentials) {
      this.sessionCredentials = sessionCredentials;
      return this;
    }

    /**
     * Supplies session credentials through a callback that is invoked again whenever the current
     * credentials need renewing, which keeps a long-lived client working past the lifetime of any
     * single set of credentials. Populate {@link StsCredentials#getExpiration()} so renewal can be
     * scheduled ahead of expiry; otherwise renewal falls back to a fixed interval.
     *
     * <p>The supplier is called from request threads and must be thread-safe and reasonably fast.
     *
     * @param sessionCredentialsSupplier callback returning currently valid session credentials
     * @return this builder
     */
    public Builder withSessionCredentialsSupplier(
        Supplier<StsCredentials> sessionCredentialsSupplier) {
      this.sessionCredentialsSupplier = sessionCredentialsSupplier;
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

    public CredentialsOverrider build() {
      return new CredentialsOverrider(this);
    }
  }
}
