package com.salesforce.multicloudj.registry.provider;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;

/**
 * AWS ECR (Elastic Container Registry) implementation.
 *
 * Registry Endpoint: https://{accountId}.dkr.ecr.{region}.amazonaws.com
 * Auth: Basic Auth with username "AWS" and token from ECR GetAuthorizationToken API
 */
@AutoService(AbstractRegistry.class)
public class AwsRegistry extends AbstractRegistry {

    public AwsRegistry() {
        this(new Builder(), null);
    }

    public AwsRegistry(Builder builder, OciRegistryClient ociClient) {
        super(builder);
        this.ociClient = ociClient;
    }

    @Override
    public String getAuthToken() throws IOException {
        // Call AWS ECR GetAuthorizationToken API to get the authorization token
        // This token is used for Basic Auth (username="AWS", password=<token>)
        //
        // TODO: Implement actual ECR GetAuthorizationToken call using AWS SDK
        // Example:
        //   AmazonECRClient ecrClient = new AmazonECRClient(credentials);
        //   GetAuthorizationTokenRequest request = new GetAuthorizationTokenRequest();
        //   GetAuthorizationTokenResult result = ecrClient.getAuthorizationToken(request);
        //   return result.getAuthorizationData().get(0).getAuthorizationToken();
        //
        // For now, placeholder
        throw new UnsupportedOperationException("ECR GetAuthorizationToken not yet implemented. " +
            "Need to integrate with AWS SDK to call GetAuthorizationToken API.");
    }

    @Override
    public String getAuthUsername() {
        // AWS ECR uses "AWS" as the username for Basic Auth
        return "AWS";
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        // TODO: Map AWS-specific exceptions to MultiCloudJ common exceptions
        // Example: if (t instanceof AmazonServiceException) return AwsSdkException.class;
        return SubstrateSdkException.class; // Generic fallback
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    /**
     * Builder for AwsRegistry.
     * Creates OciRegistryClient in build() method, following MultiCloudJ pattern.
     */
    public static class Builder extends AbstractRegistry.Builder<AwsRegistry, Builder> {

        private OciRegistryClient ociClient;

        public Builder() {
            providerId(AwsConstants.PROVIDER_ID);
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public AwsRegistry build() {
            if (ociClient == null) {
                // Create AwsRegistry instance first (ociClient will be null initially)
                AwsRegistry registry = new AwsRegistry(this, null);
                
                // Now create OciRegistryClient using the registry instance as AuthProvider
                CloseableHttpClient httpClient = registry.getHttpClient();
                ociClient = new OciRegistryClient(getRegistryEndpoint(), registry, httpClient);
                
                // Set the ociClient on the registry instance
                registry.ociClient = ociClient;
                
                return registry;
            }
            return new AwsRegistry(this, ociClient);
        }
    }
}
