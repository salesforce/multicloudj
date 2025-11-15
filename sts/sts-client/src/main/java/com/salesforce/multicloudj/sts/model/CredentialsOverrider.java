package com.salesforce.multicloudj.sts.model;

import lombok.Builder;
import lombok.Getter;

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
@Builder
public class CredentialsOverrider {
    protected CredentialsType type;
    protected StsCredentials sessionCredentials;
    protected String role;
    protected Integer durationSeconds;
    protected String sessionName;
}
