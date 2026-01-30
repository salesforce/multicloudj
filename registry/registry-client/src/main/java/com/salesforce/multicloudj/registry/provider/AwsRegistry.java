package com.salesforce.multicloudj.registry.provider;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.registry.driver.AbstractRegistry;
import com.salesforce.multicloudj.registry.driver.OciRegistryClient;
import org.apache.http.impl.client.CloseableHttpClient;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ecr.EcrClient;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenRequest;
import software.amazon.awssdk.services.ecr.model.GetAuthorizationTokenResponse;

import java.io.IOException;

/**
 * AWS ECR (Elastic Container Registry) implementation.
 *
 * Registry Endpoint: https://{accountId}.dkr.ecr.{region}.amazonaws.com
 * Auth: Basic Auth with username "AWS" and token from ECR GetAuthorizationToken API
 */
@AutoService(AbstractRegistry.class)
public class AwsRegistry extends AbstractRegistry {
    private final EcrClient ecrClient;

    public AwsRegistry() {
        this(new Builder(), null, null);
    }

    public AwsRegistry(Builder builder, OciRegistryClient ociClient, EcrClient ecrClient) {
        super(builder);
        this.ociClient = ociClient;
        this.ecrClient = ecrClient;
    }

    @Override
    public String getAuthToken() throws IOException {
        // Use the pre-initialized EcrClient (created in Builder.build())
        GetAuthorizationTokenResponse response = ecrClient.getAuthorizationToken(
            GetAuthorizationTokenRequest.builder().build()
        );
        
        // ECR returns base64-encoded "AWS:token", we need to extract the token part
        String authToken = response.authorizationData().get(0).authorizationToken();
        byte[] decodedBytes = java.util.Base64.getDecoder().decode(authToken);
        String decodedAuth = new String(decodedBytes, java.nio.charset.StandardCharsets.UTF_8);
        
        // Remove "AWS:" prefix to get the actual token
        if (decodedAuth.startsWith("AWS:")) {
            return decodedAuth.substring(4);
        }
        return decodedAuth; // Should not happen for ECR
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
     * Creates EcrClient and OciRegistryClient in build() method, following MultiCloudJ pattern.
     */
    public static class Builder extends AbstractRegistry.Builder<AwsRegistry, Builder> {

        private OciRegistryClient ociClient;
        private EcrClient ecrClient;

        public Builder() {
            providerId(AwsConstants.PROVIDER_ID);
        }

        @Override
        public Builder self() {
            return this;
        }

        /**
         * Helper method to build EcrClient using credentialsOverrider (if provided) or default credentials.
         */
        private static EcrClient buildEcrClient(Builder builder) {
            Region regionObj = Region.of(builder.getRegion());
            EcrClient.Builder ecrBuilder = EcrClient.builder().region(regionObj);

            // Use credentialsOverrider if provided, otherwise use default credentials chain
            AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(
                builder.getCredentialsOverrider(), regionObj);
            if (credentialsProvider != null) {
                ecrBuilder.credentialsProvider(credentialsProvider);
            } else {
                // Use default credentials chain
                ecrBuilder.credentialsProvider(DefaultCredentialsProvider.create());
            }

            return ecrBuilder.build();
        }

        @Override
        public AwsRegistry build() {
            if (ecrClient == null) {
                ecrClient = buildEcrClient(this);
            }

            if (ociClient == null) {
                // Create AwsRegistry instance first (ociClient will be null initially)
                AwsRegistry registry = new AwsRegistry(this, null, ecrClient);
                
                // Now create OciRegistryClient using the registry instance as AuthProvider
                CloseableHttpClient httpClient = registry.getHttpClient();
                ociClient = new OciRegistryClient(getRegistryEndpoint(), registry, httpClient);
                
                // Set the ociClient on the registry instance
                registry.ociClient = ociClient;
                
                return registry;
            }
            return new AwsRegistry(this, ociClient, ecrClient);
        }
    }
}
