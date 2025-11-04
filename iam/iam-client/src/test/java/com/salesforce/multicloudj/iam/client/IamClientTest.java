package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
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
        when(mockIam.getProviderId()).thenReturn("test");

        ServiceLoader<AbstractIam> serviceLoader = mock(ServiceLoader.class);
        when(serviceLoader.iterator()).thenAnswer(invocation -> List.of(mockIam).iterator());

        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(serviceLoader);

        client = IamClient.builder("test")
                .withRegion("testRegion")
                .withEndpoint(URI.create("https://iam.endpoint.com"))
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
        String expectedId = "test-identity-id";
        when(mockIam.createIdentity(anyString(), anyString(), anyString(), anyString(), any(), any()))
                .thenReturn(expectedId);

        String result = client.createIdentity("TestRole", "Test role", "123456789012", 
                "testRegion", Optional.empty(), Optional.empty());

        assertEquals(expectedId, result);
        verify(mockIam, times(1)).createIdentity(
                eq("TestRole"), eq("Test role"), eq("123456789012"), 
                eq("testRegion"), eq(Optional.empty()), eq(Optional.empty()));
    }

    @Test
    void testCreateIdentityThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).createIdentity(
                anyString(), anyString(), anyString(), anyString(), any(), any());

        assertThrows(UnAuthorizedException.class, () -> 
                client.createIdentity("TestRole", "Test role", "123456789012", 
                        "testRegion", Optional.empty(), Optional.empty()));
    }

    @Test
    void testAttachInlinePolicy() {
        PolicyDocument policy = mock(PolicyDocument.class);
        
        client.attachInlinePolicy(policy, "123456789012", "testRegion", "test-resource");

        verify(mockIam, times(1)).attachInlinePolicy(
                eq(policy), eq("123456789012"), eq("testRegion"), eq("test-resource"));
    }

    @Test
    void testAttachInlinePolicyThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        PolicyDocument policy = mock(PolicyDocument.class);
        doThrow(RuntimeException.class).when(mockIam).attachInlinePolicy(
                any(), anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.attachInlinePolicy(policy, "123456789012", "testRegion", "test-resource"));
    }

    @Test
    void testGetInlinePolicyDetails() {
        String expectedDetails = "policy-details";
        when(mockIam.getInlinePolicyDetails(anyString(), anyString(), anyString(), anyString()))
                .thenReturn(expectedDetails);

        String result = client.getInlinePolicyDetails("TestRole", "TestPolicy", 
                "123456789012", "testRegion");

        assertEquals(expectedDetails, result);
        verify(mockIam, times(1)).getInlinePolicyDetails(
                eq("TestRole"), eq("TestPolicy"), eq("123456789012"), eq("testRegion"));
    }

    @Test
    void testGetInlinePolicyDetailsThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).getInlinePolicyDetails(
                anyString(), anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.getInlinePolicyDetails("TestRole", "TestPolicy", "123456789012", "testRegion"));
    }

    @Test
    void testGetAttachedPolicies() {
        List<String> expectedPolicies = Arrays.asList("policy1", "policy2");
        when(mockIam.getAttachedPolicies(anyString(), anyString(), anyString()))
                .thenReturn(expectedPolicies);

        List<String> result = client.getAttachedPolicies("TestRole", "123456789012", "testRegion");

        assertEquals(expectedPolicies, result);
        verify(mockIam, times(1)).getAttachedPolicies(
                eq("TestRole"), eq("123456789012"), eq("testRegion"));
    }

    @Test
    void testGetAttachedPoliciesThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).getAttachedPolicies(
                anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.getAttachedPolicies("TestRole", "123456789012", "testRegion"));
    }

    @Test
    void testRemovePolicy() {
        client.removePolicy("TestRole", "TestPolicy", "123456789012", "testRegion");

        verify(mockIam, times(1)).removePolicy(
                eq("TestRole"), eq("TestPolicy"), eq("123456789012"), eq("testRegion"));
    }

    @Test
    void testRemovePolicyThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).removePolicy(
                anyString(), anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.removePolicy("TestRole", "TestPolicy", "123456789012", "testRegion"));
    }

    @Test
    void testDeleteIdentity() {
        client.deleteIdentity("TestRole", "123456789012", "testRegion");

        verify(mockIam, times(1)).deleteIdentity(
                eq("TestRole"), eq("123456789012"), eq("testRegion"));
    }

    @Test
    void testDeleteIdentityThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).deleteIdentity(
                anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.deleteIdentity("TestRole", "123456789012", "testRegion"));
    }

    @Test
    void testGetIdentity() {
        String expectedArn = "arn:aws:iam::123456789012:role/TestRole";
        when(mockIam.getIdentity(anyString(), anyString(), anyString()))
                .thenReturn(expectedArn);

        String result = client.getIdentity("TestRole", "123456789012", "testRegion");

        assertEquals(expectedArn, result);
        verify(mockIam, times(1)).getIdentity(
                eq("TestRole"), eq("123456789012"), eq("testRegion"));
    }

    @Test
    void testGetIdentityThrowsException() {
        doReturn(UnAuthorizedException.class).when(mockIam).getException(any());
        doThrow(RuntimeException.class).when(mockIam).getIdentity(
                anyString(), anyString(), anyString());

        assertThrows(UnAuthorizedException.class, () -> 
                client.getIdentity("TestRole", "123456789012", "testRegion"));
    }

    @Test
    void testBuilderWithInvalidProvider() {
        // Close the existing mock and create a new one
        if (serviceLoaderStatic != null) {
            serviceLoaderStatic.close();
        }
        
        ServiceLoader<AbstractIam> emptyLoader = mock(ServiceLoader.class);
        when(emptyLoader.iterator()).thenAnswer(invocation -> List.of().iterator());
        
        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(emptyLoader);
        
        assertThrows(IllegalArgumentException.class, () -> 
                IamClient.builder("nonexistent"));
    }

    @Test
    void testBuilderMethodChaining() {
        IamClient.IamClientBuilder builder = IamClient.builder("test");
        
        IamClient.IamClientBuilder result1 = builder.withRegion("testRegion");
        assertEquals(builder, result1);
        
        IamClient.IamClientBuilder result2 = builder.withEndpoint(URI.create("https://test.com"));
        assertEquals(builder, result2);
        
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
        IamClient testClient = IamClient.builder("mockProviderId")
                .withRegion("testRegion")
                .withEndpoint(URI.create("https://test.endpoint.com"))
                .build();
        
        assertNotNull(testClient);
        assertNotNull(testClient.iam);
        assertEquals("mockProviderId", testClient.iam.getProviderId());
    }

    @Test
    void testBuilderFields() {
        IamClient.IamClientBuilder builder = IamClient.builder("test");
        
        builder.withRegion("testRegion");
        builder.withEndpoint(URI.create("https://custom.endpoint.com"));
        
        assertEquals("testRegion", builder.region);
        assertEquals(URI.create("https://custom.endpoint.com"), builder.endpoint);
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
        when(testIam.getProviderId()).thenReturn("test");
        
        ServiceLoader<AbstractIam> testLoader = mock(ServiceLoader.class);
        when(testLoader.iterator()).thenAnswer(invocation -> List.of(testIam).iterator());
        
        serviceLoaderStatic = mockStatic(ServiceLoader.class);
        serviceLoaderStatic.when(() -> ServiceLoader.load(AbstractIam.class)).thenReturn(testLoader);
        
        CredentialsOverrider mockOverrider = mock(CredentialsOverrider.class);
        
        IamClient builtClient = IamClient.builder("test")
                .withRegion("testRegion")
                .withCredentialsOverrider(mockOverrider)
                .build();
        
        assertNotNull(builtClient);
        assertNotNull(builtClient.iam);
    }
}

