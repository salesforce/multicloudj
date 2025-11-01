package com.salesforce.multicloudj.iam.client;

import com.google.common.collect.ImmutableSet;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

/**
 * Entry point for client code to interact with Identity and Access Management (IAM) services
 * in a substrate-agnostic way.
 *
 * <p>This client provides unified IAM operations across multiple cloud providers including
 * AWS IAM, GCP IAM, and AliCloud RAM. It handles the complexity of different cloud IAM models
 * and provides a consistent API for identity lifecycle management and policy operations.
 *
 * <p>Usage example:
 * <pre>
 * IamClient client = IamClient.builder("aws")
 *     .withRegion("us-west-2")
 *     .build();
 *
 * // Create identity
 * String identityId = client.createIdentity("MyRole", "Example role", "123456789012", "us-west-2",
 *     Optional.empty(), Optional.empty());
 *
 * // Create policy
 * PolicyDocument policy = PolicyDocument.builder()
 *     .version("2012-10-17")  // Use provider-specific version (AWS example)
 *     .statement("StorageAccess")
 *         .effect("Allow")
 *         .addAction("storage:GetObject")
 *         .addResource("storage://my-bucket/*")
 *     .endStatement()
 *     .build();
 *
 * // Attach policy
 * client.attachInlinePolicy(policy, "123456789012", "us-west-2", "my-bucket");
 * </pre>
 */
public class IamClient {
    protected AbstractIam<?> iam;

    /**
     * Constructor for IamClient with IamClientBuilder.
     *
     * @param iam The abstract IAM driver used to back this client for implementation.
     */
    protected IamClient(AbstractIam<?> iam) {
        this.iam = iam;
    }

    /**
     * Creates a new IamClientBuilder for the specified provider.
     *
     * @param providerId the ID of the provider such as "aws", "gcp", or "ali"
     * @return a new IamClientBuilder instance
     */
    public static IamClientBuilder builder(String providerId) {
        return new IamClientBuilder(providerId);
    }

    /**
     * Returns an Iterable of all available AbstractIam implementations.
     * @return An Iterable of AbstractIam instances.
     */
    private static Iterable<AbstractIam<?>> all() {
        ServiceLoader<AbstractIam> services = ServiceLoader.load(AbstractIam.class);
        ImmutableSet.Builder<AbstractIam<?>> builder = ImmutableSet.builder();
        for (AbstractIam<?> service : services) {
            builder.add(service);
        }
        return builder.build();
    }

    /**
     * Finds the builder for the specified provider.
     * @param providerId The ID of the provider.
     * @return The AbstractIam.Builder for the specified provider.
     * @throws IllegalArgumentException if no provider is found for the given ID.
     */
    private static AbstractIam.Builder<?> findProviderBuilder(String providerId) {
        for (AbstractIam<?> provider : all()) {
            if (provider.getProviderId().equals(providerId)) {
                return createBuilderInstance(provider);
            }
        }
        throw new IllegalArgumentException("No cloud provider found for providerId: " + providerId);
    }

    /**
     * Creates a builder instance for the given provider.
     * @param provider The AbstractIam provider.
     * @return The AbstractIam.Builder for the provider.
     * @throws RuntimeException if the builder creation fails.
     */
    private static AbstractIam.Builder<?> createBuilderInstance(AbstractIam<?> provider) {
        try {
            return (AbstractIam.Builder<?>) provider.getClass().getMethod("builder").invoke(provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create builder for provider: " + provider.getClass().getName(), e);
        }
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
        try {
            return this.iam.createIdentity(identityName, description, tenantId, region, trustConfig, options);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
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
        try {
            this.iam.attachInlinePolicy(policyDocument, tenantId, region, resource);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
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
        try {
            return this.iam.getInlinePolicyDetails(identityName, policyName, tenantId, region);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
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
        try {
            return this.iam.getAttachedPolicies(identityName, tenantId, region);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
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
        try {
            this.iam.removePolicy(identityName, policyName, tenantId, region);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
    }

    /**
     * Deletes an identity from the cloud provider.
     *
     * @param identityName the name of the identity to delete
     * @param tenantId the tenant ID
     * @param region the region
     */
    public void deleteIdentity(String identityName, String tenantId, String region) {
        try {
            this.iam.deleteIdentity(identityName, tenantId, region);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
        }
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
        try {
            return this.iam.getIdentity(identityName, tenantId, region);
        } catch (Throwable t) {
            Class<? extends SubstrateSdkException> exception = this.iam.getException(t);
            ExceptionHandler.handleAndPropagate(exception, t);
            return null;
        }
    }

    /**
     * Builder class for IamClient.
     */
    public static class IamClientBuilder {
        protected String region;
        protected URI endpoint;
        protected AbstractIam<?> iam;
        protected AbstractIam.Builder<?> iamBuilder;

        /**
         * Constructor for IamClientBuilder.
         *
         * @param providerId the ID of the provider such as "aws", "gcp", or "ali"
         */
        public IamClientBuilder(String providerId) {
            this.iamBuilder = findProviderBuilder(providerId);
        }

        /**
         * Sets the region for the IAM client.
         *
         * @param region the region to set
         * @return this IamClientBuilder instance
         */
        public IamClientBuilder withRegion(String region) {
            this.region = region;
            this.iamBuilder.withRegion(region);
            return this;
        }

        /**
         * Sets the endpoint to override for the IAM client.
         *
         * @param endpoint the endpoint to set
         * @return this IamClientBuilder instance
         */
        public IamClientBuilder withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            this.iamBuilder.withEndpoint(endpoint);
            return this;
        }

        /**
         * Builds and returns an IamClient instance.
         *
         * @return a new IamClient instance
         */
        public IamClient build() {
            this.iam = this.iamBuilder.build();
            return new IamClient(this.iam);
        }
    }
}