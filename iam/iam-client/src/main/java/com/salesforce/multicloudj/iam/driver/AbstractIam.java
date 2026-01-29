package com.salesforce.multicloudj.iam.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import lombok.Getter;

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
        if (identityName == null || identityName.trim().isEmpty()) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (tenantId == null || tenantId.trim().isEmpty()) {
            throw new InvalidArgumentException("tenantId is required");
        }
        return doCreateIdentity(identityName, description, tenantId, region, trustConfig, options);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void attachInlinePolicy(PolicyDocument policyDocument, String tenantId,
                                   String region, String resource) {
        doAttachInlinePolicy(policyDocument, tenantId, region, resource);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getInlinePolicyDetails(GetInlinePolicyDetailsRequest request) {
        if (request == null) {
            throw new InvalidArgumentException("request cannot be null");
        }
        return doGetInlinePolicyDetails(
                request.getIdentityName(),
                request.getPolicyName(),
                request.getRoleName(),
                request.getTenantId(),
                request.getRegion()
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getAttachedPolicies(GetAttachedPoliciesRequest request) {
        if (request == null) {
            throw new InvalidArgumentException("request cannot be null");
        }
        if (request.getIdentityName() == null || request.getIdentityName().trim().isEmpty()) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (request.getTenantId() == null || request.getTenantId().trim().isEmpty()) {
            throw new InvalidArgumentException("tenantId is required");
        }
        return doGetAttachedPolicies(request.getIdentityName(), request.getTenantId(), request.getRegion());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePolicy(String identityName, String policyName, String tenantId, String region) {
        if (identityName == null || identityName.trim().isEmpty()) {
            throw new InvalidArgumentException("identityName is required");
        }
        if (policyName == null || policyName.trim().isEmpty()) {
            throw new InvalidArgumentException("policyName is required");
        }
        doRemovePolicy(identityName, policyName, tenantId, region);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteIdentity(String identityName, String tenantId, String region) {
        if (identityName == null || identityName.trim().isEmpty()) {
            throw new InvalidArgumentException("identityName is required");
        }
        doDeleteIdentity(identityName, tenantId, region);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getIdentity(String identityName, String tenantId, String region) {
        if (identityName == null || identityName.trim().isEmpty()) {
            throw new InvalidArgumentException("identityName is required");
        }
        return doGetIdentity(identityName, tenantId, region);
    }

    // Protected abstract methods to be implemented by provider-specific classes

    /**
     * Creates a new identity (role/service account) in the cloud provider.
     * Provider-specific implementations should override this method.
     *
     * @param identityName the name of the identity to create
     * @param description optional description for the identity (can be null)
     * @param tenantId the tenant ID (AWS Account ID, GCP Project ID, or AliCloud Account ID)
     * @param region the region for IAM operations
     * @param trustConfig optional trust configuration
     * @param options optional creation options
     * @return the unique identifier of the created identity
     */
    protected abstract String doCreateIdentity(String identityName, String description, String tenantId,
                                              String region, Optional<TrustConfiguration> trustConfig,
                                              Optional<CreateOptions> options);

    /**
     * Attaches an inline policy to a resource.
     * Provider-specific implementations should override this method.
     *
     * @param policyDocument the policy document in substrate-neutral format
     * @param tenantId the tenant ID
     * @param region the region
     * @param resource the resource to attach the policy to
     */
    protected abstract void doAttachInlinePolicy(PolicyDocument policyDocument, String tenantId,
                                                String region, String resource);

    /**
     * Retrieves the details of a specific inline policy attached to an identity.
     * Provider-specific implementations should override this method.
     *
     * @param identityName the identity name
     * @param policyName the policy name (optional and subject to cloud semantics)
     * @param roleName the role name (optional and subject to cloud semantics)
     * @param tenantId the tenant ID
     * @param region the region
     * @return the policy document details as a string
     */
    protected abstract String doGetInlinePolicyDetails(String identityName, String policyName, String roleName, String tenantId, String region);

    /**
     * Lists all inline policies attached to an identity.
     * Provider-specific implementations should override this method.
     *
     * @param identityName the identity name
     * @param tenantId the tenant ID
     * @param region the region
     * @return a list of policy names
     */
    protected abstract List<String> doGetAttachedPolicies(String identityName, String tenantId, String region);

    /**
     * Removes an inline policy from an identity.
     * Provider-specific implementations should override this method.
     *
     * @param identityName the name of the identity
     * @param policyName the name of the policy to remove
     * @param tenantId the tenant ID
     * @param region the region
     */
    protected abstract void doRemovePolicy(String identityName, String policyName, String tenantId, String region);

    /**
     * Deletes an identity from the cloud provider.
     * Provider-specific implementations should override this method.
     *
     * @param identityName the name of the identity to delete
     * @param tenantId the tenant ID
     * @param region the region
     */
    protected abstract void doDeleteIdentity(String identityName, String tenantId, String region);

    /**
     * Retrieves metadata about an identity.
     * Provider-specific implementations should override this method.
     *
     * @param identityName the name of the identity
     * @param tenantId the tenant ID
     * @param region the region
     * @return the unique identity identifier (ARN, email, or roleId)
     */
    protected abstract String doGetIdentity(String identityName, String tenantId, String region);
}
