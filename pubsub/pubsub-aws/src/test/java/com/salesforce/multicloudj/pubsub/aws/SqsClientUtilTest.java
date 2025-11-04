package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SqsClientUtilTest {

    private MockedStatic<SqsClient> sqsClientStatic;
    private MockedStatic<CredentialsProvider> credentialsProviderStatic;
    private SqsClientBuilder mockBuilder;
    private SqsClient mockSqsClient;
    private AwsCredentialsProvider mockCredentialsProvider;

    @BeforeEach
    void setUp() {
        // Mock SqsClient.builder()
        mockBuilder = mock(SqsClientBuilder.class);
        mockSqsClient = mock(SqsClient.class);
        mockCredentialsProvider = mock(AwsCredentialsProvider.class);

        sqsClientStatic = mockStatic(SqsClient.class);
        sqsClientStatic.when(SqsClient::builder).thenReturn(mockBuilder);

        // Mock builder chain methods
        when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
        when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
        when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockSqsClient);

        // Mock CredentialsProvider.getCredentialsProvider()
        credentialsProviderStatic = mockStatic(CredentialsProvider.class);
    }

    @AfterEach
    void tearDown() {
        if (sqsClientStatic != null) {
            sqsClientStatic.close();
        }
        if (credentialsProviderStatic != null) {
            credentialsProviderStatic.close();
        }
    }

    @Test
    void testBuildSqsClient_WithRegionOnly() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder).region(Region.of(region));
        verify(mockBuilder, never()).endpointOverride(any(URI.class));
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_WithEndpointOnly() {
        String region = null;
        URI endpoint = URI.create("https://sqs.us-east-1.amazonaws.com");
        CredentialsOverrider credentialsOverrider = null;

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder, never()).region(any(Region.class));
        verify(mockBuilder).endpointOverride(endpoint);
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_WithCredentialsOnly() {
        String region = null;
        URI endpoint = null;
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any())).thenReturn(mockCredentialsProvider);

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder, never()).region(any(Region.class));
        verify(mockBuilder, never()).endpointOverride(any(URI.class));
        credentialsProviderStatic.verify(() -> CredentialsProvider.getCredentialsProvider(
                credentialsOverrider, null));
        verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_WithAllParameters() {
        String region = "us-west-2";
        URI endpoint = URI.create("https://sqs.us-west-2.amazonaws.com");
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class))).thenReturn(mockCredentialsProvider);

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder).region(Region.of(region));
        verify(mockBuilder).endpointOverride(endpoint);
        credentialsProviderStatic.verify(() -> CredentialsProvider.getCredentialsProvider(
                credentialsOverrider, Region.of(region)));
        verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_WithNullCredentialsProvider() {
        String region = "us-east-1";
        URI endpoint = null;
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class))).thenReturn(null);

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder).region(Region.of(region));
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_WithAllNullParameters() {
        String region = null;
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder, never()).region(any(Region.class));
        verify(mockBuilder, never()).endpointOverride(any(URI.class));
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_WithAssumeRoleCredentials() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
                .withRole("test-role")
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class))).thenReturn(mockCredentialsProvider);

        SqsClient result = SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSqsClient, result);
        verify(mockBuilder).region(Region.of(region));
        credentialsProviderStatic.verify(() -> CredentialsProvider.getCredentialsProvider(
                credentialsOverrider, Region.of(region)));
        verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSqsClient_ThrowsException_WhenBuilderFails() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        when(mockBuilder.region(any(Region.class))).thenThrow(new RuntimeException("Builder failed"));

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);
        });

        assertEquals("Failed to create SQS client", exception.getMessage());
        assertNotNull(exception.getCause());
        assertEquals("Builder failed", exception.getCause().getMessage());
    }

    @Test
    void testBuildSqsClient_ThrowsException_WhenCredentialsProviderFails() {
        String region = "us-east-1";
        URI endpoint = null;
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class)))
                .thenThrow(new RuntimeException("Credentials provider failed"));

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);
        });

        assertEquals("Failed to create SQS client", exception.getMessage());
        assertNotNull(exception.getCause());
        assertEquals("Credentials provider failed", exception.getCause().getMessage());
    }

    @Test
    void testBuildSqsClient_ThrowsException_WhenBuildFails() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        when(mockBuilder.build()).thenThrow(new RuntimeException("Build failed"));

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            SqsClientUtil.buildSqsClient(region, endpoint, credentialsOverrider);
        });

        assertEquals("Failed to create SQS client", exception.getMessage());
        assertNotNull(exception.getCause());
        assertEquals("Build failed", exception.getCause().getMessage());
    }
}

