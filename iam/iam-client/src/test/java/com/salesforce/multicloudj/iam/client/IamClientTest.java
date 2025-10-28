package com.salesforce.multicloudj.iam.client;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for IamClient builder pattern and basic functionality.
 */
public class IamClientTest {

    @Test
    public void testIamClientBuilder() {
        // Test builder creation with different provider IDs
        IamClient.IamClientBuilder builder = IamClient.builder("aws");
        assertNotNull(builder);

        // Test method chaining
        IamClient.IamClientBuilder result = builder
            .withRegion("us-west-2")
            .withEndpoint(URI.create("https://iam.amazonaws.com"));

        assertSame(builder, result, "Builder methods should return the same instance for chaining");
    }

    @Test
    public void testIamClientBuilderWithDifferentProviders() {
        // Test with AWS
        IamClient.IamClientBuilder awsBuilder = IamClient.builder("aws");
        assertNotNull(awsBuilder);

        // Test with GCP
        IamClient.IamClientBuilder gcpBuilder = IamClient.builder("gcp");
        assertNotNull(gcpBuilder);

        // Test with AliCloud
        IamClient.IamClientBuilder aliBuilder = IamClient.builder("ali");
        assertNotNull(aliBuilder);
    }

    @Test
    public void testIamClientBuild() {
        IamClient client = IamClient.builder("aws")
            .withRegion("us-east-1")
            .build();

        assertNotNull(client);
    }

    @Test
    public void testIamClientBuildWithEndpoint() {
        URI customEndpoint = URI.create("https://custom-iam-endpoint.com");

        IamClient client = IamClient.builder("gcp")
            .withRegion("us-central1")
            .withEndpoint(customEndpoint)
            .build();

        assertNotNull(client);
    }

}