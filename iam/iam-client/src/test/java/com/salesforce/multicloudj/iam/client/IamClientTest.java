package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class IamClientTest {

    // Test constants
    private static final String TEST_PROVIDER_ID = "test";
    private static final String TEST_REGION = "testRegion";
    private static final String TEST_ROLE = "TestRole";
    private static final String TEST_ROLE_DESCRIPTION = "Test role description";
    private static final String TEST_TENANT_ID = "123456789012";
    private static final String TEST_POLICY_NAME = "TestPolicy";
    private static final String TEST_RESOURCE = "test-resource";
    private static final String TEST_IDENTITY_ID = "test-identity-id";
    private static final String TEST_IDENTITY_ARN = "arn:aws:iam::123456789012:role/TestRole";
    private static final String POLICY_RESPONSE = "policy-details";
    private static final URI TEST_ENDPOINT = URI.create("https://test.endpoint.com");

    private AbstractIam<?> mockIam;
    private IamClient client;
    private MockedStatic<ServiceLoader> serviceLoaderStatic;

    @BeforeEach
    void setup() {
        mockIam = mock(TestIam.class);
        
        TestIam.Builder mockBuilder = mock(TestIam.Builder.class);
        when(mockBuilder.withRegion(anyString())).thenReturn(mockBuilder);
        when(mockBuilder.withEndpoint(any(URI.class))).thenReturn(mockBuilder);
        when(mockBuilder.build()).thenReturn((TestIam) mockIam);
        when(mockIam.builder()).thenReturn(mockBuilder);
        when(mockIam.getProviderId()).thenReturn(TEST_PROVIDER_ID);

        ServiceLoader<AbstractIam> serviceLoader = mock(ServiceLoader.class);
        when(serviceLoader.iterator()).thenAnswer(invocation -> List.of(mockIam).iterator());

        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(serviceLoader);

        client = IamClient.builder(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .withEndpoint(TEST_ENDPOINT)
                .build();
    }

    @AfterEach
    void teardown() {
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }
    }
    @Test
    void testCreateIdentity() {
        when(mockIam.createIdentity(anyString(), anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(TEST_IDENTITY_ID);

        String result = client.createIdentity(TEST_ROLE, TEST_ROLE_DESCRIPTION, TEST_TENANT_ID, 
                TEST_REGION, Optional.empty(), Optional.empty());

        assertEquals(TEST_IDENTITY_ID, result);
        verify(mockIam, times(1)).createIdentity(
                eq(TEST_ROLE), eq(TEST_ROLE_DESCRIPTION), eq(TEST_TENANT_ID), 
                eq(TEST_REGION), eq(Optional.empty()), eq(Optional.empty()));
    }

    @Test
    void testCreateIdentityThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).createIdentity(
                anyString(), anyString(), anyString(), anyString(), any(), any());

        assertThrows(UnAuthorizedException.class, () -> 
                client.createIdentity(TEST_ROLE, TEST_ROLE_DESCRIPTION, TEST_TENANT_ID, 
                        TEST_REGION, Optional.empty(), Optional.empty()));
    }

    @Test
    void testAttachInlinePolicy() {
        PolicyDocument policy = mock(PolicyDocument.class);
        
        client.attachInlinePolicy(policy, TEST_TENANT_ID, TEST_REGION, TEST_RESOURCE);

        verify(mockIam, times(1)).attachInlinePolicy(
                eq(policy), eq(TEST_TENANT_ID), eq(TEST_REGION), eq(TEST_RESOURCE));
    }

    @Test
    void testAttachInlinePolicyThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        PolicyDocument policy = mock(PolicyDocument.class);
        doThrow(RuntimeException.class).when(mockIam).attachInlinePolicy(
                any(), anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.attachInlinePolicy(policy, TEST_TENANT_ID, TEST_REGION, TEST_RESOURCE));
    }

    @Test
    void testGetInlinePolicyDetails() {
        when(mockIam.getInlinePolicyDetails(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(POLICY_RESPONSE);

        String result = client.getInlinePolicyDetails(TEST_ROLE, TEST_POLICY_NAME, 
                TEST_TENANT_ID, TEST_REGION);

        assertEquals(POLICY_RESPONSE, result);
        verify(mockIam, times(1)).getInlinePolicyDetails(
                eq(TEST_ROLE), eq(TEST_POLICY_NAME), eq(TEST_TENANT_ID), eq(TEST_REGION));
    }

    @Test
    void testGetInlinePolicyDetailsThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).getInlinePolicyDetails(
                anyString(), anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.getInlinePolicyDetails(TEST_ROLE, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION));
    }

    @Test
    void testGetAttachedPolicies() {
        List<String> expectedPolicies = Arrays.asList("policy1", "policy2");
        when(mockIam.getAttachedPolicies(anyString(), anyString(), anyString()))
                .thenReturn(expectedPolicies);

        List<String> result = client.getAttachedPolicies(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

        assertEquals(expectedPolicies, result);
        verify(mockIam, times(1)).getAttachedPolicies(
                eq(TEST_ROLE), eq(TEST_TENANT_ID), eq(TEST_REGION));
    }

    @Test
    void testGetAttachedPoliciesThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).getAttachedPolicies(
                anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.getAttachedPolicies(TEST_ROLE, TEST_TENANT_ID, TEST_REGION));
    }

    @Test
    void testRemovePolicy() {
        client.removePolicy(TEST_ROLE, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION);

        verify(mockIam, times(1)).removePolicy(
                eq(TEST_ROLE), eq(TEST_POLICY_NAME), eq(TEST_TENANT_ID), eq(TEST_REGION));
    }

    @Test
    void testRemovePolicyThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).removePolicy(
                anyString(), anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.removePolicy(TEST_ROLE, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION));
    }

    @Test
    void testDeleteIdentity() {
        client.deleteIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

        verify(mockIam, times(1)).deleteIdentity(
                eq(TEST_ROLE), eq(TEST_TENANT_ID), eq(TEST_REGION));
    }

    @Test
    void testDeleteIdentityThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).deleteIdentity(
                anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.deleteIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION));
    }

    @Test
    void testGetIdentity() {
        when(mockIam.getIdentity(anyString(), anyString(), anyString()))
                .thenReturn(TEST_IDENTITY_ARN);

        String result = client.getIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

        assertEquals(TEST_IDENTITY_ARN, result);
        verify(mockIam, times(1)).getIdentity(
                eq(TEST_ROLE), eq(TEST_TENANT_ID), eq(TEST_REGION));
    }

    @Test
    void testGetIdentityThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).getIdentity(
                anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.getIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION));
    }


    @Test
    void testBuilderMethodChaining() {
        IamClient.IamClientBuilder builder = IamClient.builder(TEST_PROVIDER_ID);

        // Test method chaining returns same builder
        IamClient.IamClientBuilder result1 = builder.withRegion(TEST_REGION);
        assertEquals(builder, result1);

        IamClient.IamClientBuilder result2 = builder.withEndpoint(TEST_ENDPOINT);
        assertEquals(builder, result2);

        // Test fields are correctly set after chaining
        assertEquals(TEST_REGION, builder.region);
        assertEquals(TEST_ENDPOINT, builder.endpoint);

        IamClient builtClient = builder.build();
        assertNotNull(builtClient);
        assertNotNull(builtClient.iam);
    }

    @Test
    void testBuilderWithRealProvider() {
        // Close existing mock and use real ServiceLoader
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }
        
        // Create a real TestIam instance
        TestIam realProvider = new TestIam();
        
        ServiceLoader<AbstractIam> realLoader = mock(ServiceLoader.class);
        when(realLoader.iterator()).thenAnswer(invocation -> List.of(realProvider).iterator());
        
        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(realLoader);
        
        // This should exercise the real createBuilderInstance path
        // TestIam has "mockProviderId" hardcoded in its builder
        IamClient testClient = IamClient.builder("mockProviderId")
                .withRegion(TEST_REGION)
                .withEndpoint(TEST_ENDPOINT)
                .build();
        
        assertNotNull(testClient);
        assertNotNull(testClient.iam);
        assertEquals("mockProviderId", testClient.iam.getProviderId());
    }


    @Test
    void testBuilderWithCredentialsOverrider() {
        // Close existing mock since we need to create a fresh one with additional stubbing
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }

        // Create fresh mocks with credentialsOverrider support
        AbstractIam<?> testIam = mock(TestIam.class);
        TestIam.Builder testBuilder = mock(TestIam.Builder.class);

        // Only stub methods that are actually called in this test
        when(testBuilder.withRegion(anyString())).thenReturn(testBuilder);
        when(testBuilder.withCredentialsOverrider(any(CredentialsOverrider.class))).thenReturn(testBuilder);
        when(testBuilder.build()).thenReturn((TestIam) testIam);
        when(testIam.builder()).thenReturn(testBuilder);
        when(testIam.getProviderId()).thenReturn(TEST_PROVIDER_ID);

        ServiceLoader<AbstractIam> testLoader = mock(ServiceLoader.class);
        when(testLoader.iterator()).thenAnswer(invocation -> List.of(testIam).iterator());

        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(testLoader);

        CredentialsOverrider mockOverrider = mock(CredentialsOverrider.class);

        IamClient builtClient = IamClient.builder(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .withCredentialsOverrider(mockOverrider)
                .build();

        assertNotNull(builtClient);
        assertNotNull(builtClient.iam);
    }

    @Test
    void testFindProviderBuilderNoMatch() {
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }

        ServiceLoader<AbstractIam> emptyLoader = mock(ServiceLoader.class);
        when(emptyLoader.iterator()).thenAnswer(invocation -> List.of().iterator());

        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(emptyLoader);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> IamClient.builder("nonexistent-provider"));
        assertEquals("No IAM provider found for providerId: nonexistent-provider",
                exception.getMessage());
    }

}

