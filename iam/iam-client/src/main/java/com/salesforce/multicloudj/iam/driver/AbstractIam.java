package com.salesforce.multicloudj.iam.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.CreateIdentityRequest;
import com.salesforce.multicloudj.iam.model.DeleteIdentityRequest;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetIdentityRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.RemovePolicyRequest;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Abstract base class for Identity and Access Management (IAM) implementations.
 * This class is internal for SDK and all the providers for IAM implementations
 * are supposed to implement it.
 *
 * <p>This provides a unified interface for managing identities (roles/service accounts)
 * and policies across different cloud providers including AWS IAM, GCP IAM, and
 * AliCloud RAM.
 */
public abstract class AbstractIam implements Provider, Identity, AutoCloseable {
    private final String providerId;
    protected final String region;
    protected final CredentialsOverrider credentialsOverrider;

    /**
     * Constructs an AbstractIam instance using a Builder.
     *
     * @param builder The Builder instance to use for construction.
     */
    public AbstractIam(Builder<?, ?> builder) {
        this(builder.providerId, builder.region, builder.credentialsOverrider);
    }

    /**
     * Constructs an AbstractIam instance with specified provider ID, region, and credentials overrider.
     *
     * @param providerId The ID of the provider.
     * @param region The region for the IAM operations.
     * @param credentialsOverrider The credentials overrider for custom authentication.
     */
    public AbstractIam(String providerId, String region, CredentialsOverrider credentialsOverrider) {
        this.providerId = providerId;
        this.region = region;
        this.credentialsOverrider = credentialsOverrider;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProviderId() {
        return providerId;
    }


    /**
     * Abstract builder class for AbstractIam implementations.
     *
     * @param <A> The concrete AbstractIam implementation type.
     * @param <T> The concrete Builder implementation type.
     */
    public abstract static class Builder<A extends AbstractIam, T extends Builder<A, T>> implements Provider.Builder {
        @Getter
        protected String region;
        @Getter
        protected URI endpoint;
        @Getter
        protected CredentialsOverrider credentialsOverrider = null;
        protected String providerId;

        /**
         * Sets the region.
         *
         * @param region The region to set.
         * @return This Builder instance.
         */
        public T withRegion(String region) {
            this.region = region;
            return self();
        }

        /**
         * Sets the endpoint to override.
         *
         * @param endpoint The endpoint to set.
         * @return This Builder instance.
         */
        public T withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return self();
        }

        /**
         * Sets the credentials overrider.
         *
         * @param credentialsOverrider The credentials overrider to set.
         * @return This Builder instance.
         */
        public T withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.credentialsOverrider = credentialsOverrider;
            return self();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public T providerId(String providerId) {
            this.providerId = providerId;
            return self();
        }

        /**
         * Returns this builder instance with the correct type.
         *
         * @return This builder instance.
         */
        public abstract T self();

        /**
         * Builds and returns an instance of AbstractIam.
         *
         * @return An instance of AbstractIam.
         */
        public abstract A build();
    }

    // Public methods implementing the Identity interface

    /**
     * {@inheritDoc}
     */
    @Override
    public String createIdentity(String identityName, String description, String tenantId,
                                String region, Optional<TrustConfiguration> trustConfig,
                                Optional<CreateOptions> options) {
        if (StringUtils.isBlank(identityName)) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (StringUtils.isBlank(tenantId)) {
            throw new InvalidArgumentException("tenantId is required");
        }
        if (trustConfig == null) {
            throw new InvalidArgumentException("trustConfig cannot be null");
        }
        if (options == null) {
            throw new InvalidArgumentException("options cannot be null");
        }
        CreateIdentityRequest request = CreateIdentityRequest.builder()
                .identityName(identityName)
                .description(description)
                .tenantId(tenantId)
                .region(region)
                .trustConfig(trustConfig)
                .options(options)
                .build();
        if (request.getTrustConfig() == null) {
            throw new InvalidArgumentException("trustConfig cannot be null");
        }
        if (request.getOptions() == null) {
            throw new InvalidArgumentException("options cannot be null");
        }
        return doCreateIdentity(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void attachInlinePolicy(AttachInlinePolicyRequest request) {
        if (request == null) {
            throw new InvalidArgumentException("request cannot be null");
        }
        if (StringUtils.isBlank(request.getIdentityName())) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (request.getPolicyDocument() == null) {
            throw new InvalidArgumentException("policyDocument cannot be null");
        }
        doAttachInlinePolicy(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getInlinePolicyDetails(GetInlinePolicyDetailsRequest request) {
        if (request == null) {
            throw new InvalidArgumentException("request cannot be null");
        }
        return doGetInlinePolicyDetails(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAttachedPolicies(GetAttachedPoliciesRequest request) {
        if (request == null) {
            throw new InvalidArgumentException("request cannot be null");
        }
        return doGetAttachedPolicies(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePolicy(String identityName, String policyName, String tenantId, String region) {
        if (StringUtils.isBlank(identityName)) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (StringUtils.isBlank(policyName)) {
            throw new InvalidArgumentException("policyName is required");
        }
        if (StringUtils.isBlank(tenantId)) {
            throw new InvalidArgumentException("tenantId is required");
        }
        RemovePolicyRequest request = RemovePolicyRequest.builder()
                .identityName(identityName)
                .policyName(policyName)
                .tenantId(tenantId)
                .region(region)
                .build();
        doRemovePolicy(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteIdentity(String identityName, String tenantId, String region) {
        if (StringUtils.isBlank(identityName)) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (StringUtils.isBlank(tenantId)) {
            throw new InvalidArgumentException("tenantId is required");
        }
        DeleteIdentityRequest request = DeleteIdentityRequest.builder()
                .identityName(identityName)
                .tenantId(tenantId)
                .region(region)
                .build();
        doDeleteIdentity(request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIdentity(String identityName, String tenantId, String region) {
        if (StringUtils.isBlank(identityName)) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (StringUtils.isBlank(tenantId)) {
            throw new InvalidArgumentException("tenantId is required");
        }
        GetIdentityRequest request = GetIdentityRequest.builder()
                .identityName(identityName)
                .tenantId(tenantId)
                .region(region)
                .build();
        return doGetIdentity(request);
    }

    // Protected abstract methods to be implemented by provider-specific classes

    /**
     * Creates a new identity (role/service account) in the cloud provider.
     * Provider-specific implementations should override this method.
     *
     * @param request the request containing identity name, description, tenant ID, region, trust config, and options
     * @return the unique identifier of the created identity
     */
    protected abstract String doCreateIdentity(CreateIdentityRequest request);

    /**
     * Attaches an inline policy to an identity.
     * Provider-specific implementations should override this method.
     *
     * @param request the request containing policy document, tenant ID, region, and identity/role names
     */
    protected abstract void doAttachInlinePolicy(AttachInlinePolicyRequest request);

    /**
     * Retrieves the details of a specific inline policy attached to an identity.
     * Provider-specific implementations should override this method.
     * @param request the request containing relevant fields from identity name, policy name, role name, tenant ID, and region
     * @return the policy document details as a string
     */
    protected abstract String doGetInlinePolicyDetails(GetInlinePolicyDetailsRequest request);

    /**
     * Lists all inline policies attached to an identity.
     * Provider-specific implementations should override this method.
     * @param request the request containing relevant fields from identity name, tenant ID, and region
     * @return a list of policy names
     */
    protected abstract List<String> doGetAttachedPolicies(GetAttachedPoliciesRequest request);

    /**
     * Removes an inline policy from an identity.
     * Provider-specific implementations should override this method.
     *
     * @param request the request containing identity name, policy name, tenant ID, and region
     */
    protected abstract void doRemovePolicy(RemovePolicyRequest request);

    /**
     * Deletes an identity from the cloud provider.
     * Provider-specific implementations should override this method.
     *
     * @param request the request containing identity name, tenant ID, and region
     */
    protected abstract void doDeleteIdentity(DeleteIdentityRequest request);

    /**
     * Retrieves metadata about an identity.
     * Provider-specific implementations should override this method.
     *
     * @param request the request containing identity name, tenant ID, and region
     * @return the unique identity identifier (ARN, email, or roleId)
     */
    protected abstract String doGetIdentity(GetIdentityRequest request);
}
