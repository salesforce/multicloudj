package com.salesforce.multicloudj.pubsub.aws;

import com.salesforce.multicloudj.common.aws.CredentialsProvider;
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
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class SnsClientUtilTest {

    private MockedStatic<SnsClient> snsClientStatic;
    private MockedStatic<CredentialsProvider> credentialsProviderStatic;
    private SnsClientBuilder mockBuilder;
    private SnsClient mockSnsClient;
    private AwsCredentialsProvider mockCredentialsProvider;

    @BeforeEach
    void setUp() {
        // Mock SnsClient.builder()
        mockBuilder = mock(SnsClientBuilder.class);
        mockSnsClient = mock(SnsClient.class);
        mockCredentialsProvider = mock(AwsCredentialsProvider.class);

        snsClientStatic = mockStatic(SnsClient.class);
        snsClientStatic.when(SnsClient::builder).thenReturn(mockBuilder);

        // Mock builder chain methods
        when(mockBuilder.region(any(Region.class))).thenReturn(mockBuilder);
        when(mockBuilder.endpointOverride(any(URI.class))).thenReturn(mockBuilder);
        when(mockBuilder.credentialsProvider(any(AwsCredentialsProvider.class))).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn(mockSnsClient);

        // Mock CredentialsProvider.getCredentialsProvider()
        credentialsProviderStatic = mockStatic(CredentialsProvider.class);
    }

    @AfterEach
    void tearDown() {
        if (snsClientStatic != null) {
            snsClientStatic.close();
        }
        if (credentialsProviderStatic != null) {
            credentialsProviderStatic.close();
        }
    }

    @Test
    void testBuildSnsClient_WithRegionOnly() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder).region(Region.of(region));
        verify(mockBuilder, never()).endpointOverride(any(URI.class));
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_WithEndpointOnly() {
        String region = null;
        URI endpoint = URI.create("https://sns.us-east-1.amazonaws.com");
        CredentialsOverrider credentialsOverrider = null;

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder, never()).region(any(Region.class));
        verify(mockBuilder).endpointOverride(endpoint);
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_WithCredentialsOnly() {
        String region = null;
        URI endpoint = null;
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any())).thenReturn(mockCredentialsProvider);

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder, never()).region(any(Region.class));
        verify(mockBuilder, never()).endpointOverride(any(URI.class));
        credentialsProviderStatic.verify(() -> CredentialsProvider.getCredentialsProvider(
                credentialsOverrider, null));
        verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_WithAllParameters() {
        String region = "us-west-2";
        URI endpoint = URI.create("https://sns.us-west-2.amazonaws.com");
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class))).thenReturn(mockCredentialsProvider);

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder).region(Region.of(region));
        verify(mockBuilder).endpointOverride(endpoint);
        credentialsProviderStatic.verify(() -> CredentialsProvider.getCredentialsProvider(
                credentialsOverrider, Region.of(region)));
        verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_WithNullCredentialsProvider() {
        String region = "us-east-1";
        URI endpoint = null;
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class))).thenReturn(null);

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder).region(Region.of(region));
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_WithAllNullParameters() {
        String region = null;
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder, never()).region(any(Region.class));
        verify(mockBuilder, never()).endpointOverride(any(URI.class));
        verify(mockBuilder, never()).credentialsProvider(any(AwsCredentialsProvider.class));
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_WithAssumeRoleCredentials() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.ASSUME_ROLE)
                .withRole("test-role")
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class))).thenReturn(mockCredentialsProvider);

        SnsClient result = SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);

        assertNotNull(result);
        assertEquals(mockSnsClient, result);
        verify(mockBuilder).region(Region.of(region));
        credentialsProviderStatic.verify(() -> CredentialsProvider.getCredentialsProvider(
                credentialsOverrider, Region.of(region)));
        verify(mockBuilder).credentialsProvider(mockCredentialsProvider);
        verify(mockBuilder).build();
    }

    @Test
    void testBuildSnsClient_ThrowsException_WhenBuilderFails() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        when(mockBuilder.region(any(Region.class))).thenThrow(new RuntimeException("Builder failed"));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);
        });

        assertEquals("Builder failed", exception.getMessage());
    }

    @Test
    void testBuildSnsClient_ThrowsException_WhenCredentialsProviderFails() {
        String region = "us-east-1";
        URI endpoint = null;
        StsCredentials stsCredentials = new StsCredentials("key", "secret", "token");
        CredentialsOverrider credentialsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials)
                .build();

        credentialsProviderStatic.when(() -> CredentialsProvider.getCredentialsProvider(
                any(CredentialsOverrider.class), any(Region.class)))
                .thenThrow(new RuntimeException("Credentials provider failed"));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);
        });

        assertEquals("Credentials provider failed", exception.getMessage());
    }

    @Test
    void testBuildSnsClient_ThrowsException_WhenBuildFails() {
        String region = "us-east-1";
        URI endpoint = null;
        CredentialsOverrider credentialsOverrider = null;

        when(mockBuilder.build()).thenThrow(new RuntimeException("Build failed"));

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            SnsClientUtil.buildSnsClient(region, endpoint, credentialsOverrider);
        });

        assertEquals("Build failed", exception.getMessage());
    }
}

