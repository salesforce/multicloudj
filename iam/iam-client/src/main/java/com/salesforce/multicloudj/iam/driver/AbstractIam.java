package com.salesforce.multicloudj.iam.driver;

import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import java.net.URI;
import java.util.List;
import java.util.Optional;

/**
 * Abstract base class for Identity and Access Management (IAM) implementations.
 * This class is internal for SDK and all the providers for IAM implementations
 * are supposed to implement it.
 */
public abstract class AbstractIam<T extends AbstractIam<T>> implements Provider {
    private final String providerId;
    protected final String region;

    /**
     * Constructs an AbstractIam instance using a Builder.
     *
     * @param builder The Builder instance to use for construction.
     */
    public AbstractIam(Builder<T> builder) {
        this(builder.providerId, builder.region);
    }

    /**
     * Constructs an AbstractIam instance with specified provider ID and region.
     *
     * @param providerId The ID of the provider.
     * @param region The region for the IAM operations.
     */
    public AbstractIam(String providerId, String region) {
        this.providerId = providerId;
        this.region = region;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getProviderId() {
        return providerId;
    }

    /**
     * Creates a new identity (role/service account) in the cloud provider.
     *
     * @param identityName the name of the identity to create
     * @param description optional description for the identity (can be null)
     * @param tenantId the tenant ID (AWS Account ID, GCP Project ID, or AliCloud Account ID)
     * @param region the region for IAM operations
     * @param trustConfig optional trust configuration
     * @param options optional creation options
     * @return the unique identifier of the created identity
     */
    public String createIdentity(String identityName, String description, String tenantId, String region,
                                 Optional<TrustConfiguration> trustConfig, Optional<CreateOptions> options) {
        return createIdentityInProvider(identityName, description, tenantId, region, trustConfig, options);
    }

    /**
     * Attaches an inline policy to a resource.
     *
     * @param policyDocument the policy document in substrate-neutral format
     * @param tenantId the tenant ID
     * @param region the region
     * @param resource the resource to attach the policy to
     */
    public void attachInlinePolicy(PolicyDocument policyDocument, String tenantId, String region, String resource) {
        attachInlinePolicyToProvider(policyDocument, tenantId, region, resource);
    }

    /**
     * Retrieves the details of a specific inline policy attached to an identity.
     *
     * @param identityName the name of the identity
     * @param policyName the name of the policy
     * @param tenantId the tenant ID
     * @param region the region
     * @return the policy document details as a string
     */
    public String getInlinePolicyDetails(String identityName, String policyName, String tenantId, String region) {
        return getInlinePolicyDetailsFromProvider(identityName, policyName, tenantId, region);
    }

    /**
     * Lists all inline policies attached to an identity.
     *
     * @param identityName the name of the identity
     * @param tenantId the tenant ID
     * @param region the region
     * @return a list of policy names
     */
    public List<String> getAttachedPolicies(String identityName, String tenantId, String region) {
        return getAttachedPoliciesFromProvider(identityName, tenantId, region);
    }

    /**
     * Removes an inline policy from an identity.
     *
     * @param identityName the name of the identity
     * @param policyName the name of the policy to remove
     * @param tenantId the tenant ID
     * @param region the region
     */
    public void removePolicy(String identityName, String policyName, String tenantId, String region) {
        removePolicyFromProvider(identityName, policyName, tenantId, region);
    }

    /**
     * Deletes an identity from the cloud provider.
     *
     * @param identityName the name of the identity to delete
     * @param tenantId the tenant ID
     * @param region the region
     */
    public void deleteIdentity(String identityName, String tenantId, String region) {
        deleteIdentityFromProvider(identityName, tenantId, region);
    }

    /**
     * Retrieves metadata about an identity.
     *
     * @param identityName the name of the identity
     * @param tenantId the tenant ID
     * @param region the region
     * @return the unique identity identifier (ARN, email, or roleId)
     */
    public String getIdentity(String identityName, String tenantId, String region) {
        return getIdentityFromProvider(identityName, tenantId, region);
    }

    /**
     * Abstract builder class for AbstractIam implementations.
     *
     * @param <T> The concrete implementation type of AbstractIam.
     */
    public abstract static class Builder<T extends AbstractIam<T>> implements Provider.Builder {
        protected String region;
        protected URI endpoint;
        protected String providerId;

        /**
         * Gets the region.
         *
         * @return The region.
         */
        public String getRegion() {
            return region;
        }

        /**
         * Gets the endpoint override.
         *
         * @return The endpoint override.
         */
        public URI getEndpoint() {
            return endpoint;
        }

        /**
         * Sets the region.
         *
         * @param region The region to set.
         * @return This Builder instance.
         */
        public Builder<T> withRegion(String region) {
            this.region = region;
            return this;
        }

        /**
         * Sets the endpoint to override.
         *
         * @param endpoint The endpoint to set.
         * @return This Builder instance.
         */
        public Builder<T> withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder<T> providerId(String providerId) {
            this.providerId = providerId;
            return this;
        }

        /**
         * Builds and returns an instance of AbstractIam.
         *
         * @return An instance of AbstractIam.
         */
        public abstract T build();
    }

    // Abstract methods to be implemented by provider-specific classes

    /**
     * Creates an identity in the provider.
     *
     * @param identityName the name of the identity
     * @param description optional description
     * @param tenantId the tenant ID
     * @param region the region
     * @param trustConfig trust configuration
     * @param options creation options
     * @return the unique identifier of the created identity
     */
    protected abstract String createIdentityInProvider(String identityName, String description, String tenantId,
                                                      String region, Optional<TrustConfiguration> trustConfig,
                                                      Optional<CreateOptions> options);

    /**
     * Attaches an inline policy to a resource in the provider.
     *
     * @param policyDocument the policy document
     * @param tenantId the tenant ID
     * @param region the region
     * @param resource the resource
     */
    protected abstract void attachInlinePolicyToProvider(PolicyDocument policyDocument, String tenantId,
                                                         String region, String resource);

    /**
     * Gets inline policy details from the provider.
     *
     * @param identityName the identity name
     * @param policyName the policy name
     * @param tenantId the tenant ID
     * @param region the region
     * @return the policy details
     */
    protected abstract String getInlinePolicyDetailsFromProvider(String identityName, String policyName,
                                                                 String tenantId, String region);

    /**
     * Gets attached policies from the provider.
     *
     * @param identityName the identity name
     * @param tenantId the tenant ID
     * @param region the region
     * @return list of policy names
     */
    protected abstract List<String> getAttachedPoliciesFromProvider(String identityName, String tenantId,
                                                                    String region);

    /**
     * Removes a policy from the provider.
     *
     * @param identityName the identity name
     * @param policyName the policy name
     * @param tenantId the tenant ID
     * @param region the region
     */
    protected abstract void removePolicyFromProvider(String identityName, String policyName, String tenantId,
                                                     String region);

    /**
     * Deletes an identity from the provider.
     *
     * @param identityName the identity name
     * @param tenantId the tenant ID
     * @param region the region
     */
    protected abstract void deleteIdentityFromProvider(String identityName, String tenantId, String region);

    /**
     * Gets identity details from the provider.
     *
     * @param identityName the identity name
     * @param tenantId the tenant ID
     * @param region the region
     * @return the identity identifier
     */
    protected abstract String getIdentityFromProvider(String identityName, String tenantId, String region);
}
