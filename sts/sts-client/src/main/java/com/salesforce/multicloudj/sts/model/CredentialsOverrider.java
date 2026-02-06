package com.salesforce.multicloudj.sts.model;

import lombok.Getter;

import java.util.function.Supplier;

/**
 * The CredentialsOverrider is used when the service wants to override the
 * default credentialsOverrider in the given environment. The default credentialsOverrider
 * are used for the majority of the use-cases and are overridden in few cases
 * including but not limited to:
 *  1. When the service wants to assume the role from another account
 *  2. Service wants to supply session credentialsOverrider for testing purposes etc.
 *  There can be more use-cases in future but for now we cover the two listed above.
 *  If the service supplies both, the session credentialsOverrider and the details for assume role,
 *  the session credentialsOverrider takes precedence over the assume role.
 */
@Getter
public class CredentialsOverrider {
    protected CredentialsType type;
    protected StsCredentials sessionCredentials;
    protected String role;
    protected Integer durationSeconds;
    protected String sessionName;
    protected Supplier<String> webIdentityTokenSupplier;
    /** When true, credential provider is pre-warmed at init to avoid interrupt on first use. Optional; default false. */
    protected boolean prewarmEnabled;

    public CredentialsOverrider(Builder builder) {
        this.type = builder.type;
        this.sessionCredentials = builder.sessionCredentials;
        this.role = builder.role;
        this.durationSeconds = builder.durationSeconds;
        this.webIdentityTokenSupplier = builder.webIdentityTokenSupplier;
        this.sessionName = builder.sessionName;
        this.prewarmEnabled = builder.prewarmEnabled;
    }

    public static class Builder {
        private final CredentialsType type;
        private StsCredentials sessionCredentials;
        private String role;
        private Integer durationSeconds;
        protected String sessionName;
        protected Supplier<String> webIdentityTokenSupplier;
        private boolean prewarmEnabled = false;

        public Builder(CredentialsType type) {
            this.type = type;
        }

        /** When true, credential provider is pre-warmed at init. Optional; default false. */
        public Builder withPrewarmEnabled(boolean prewarmEnabled) {
            this.prewarmEnabled = prewarmEnabled;
            return this;
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

        public CredentialsOverrider build() {
            return new CredentialsOverrider(this);
        }
    }
}
