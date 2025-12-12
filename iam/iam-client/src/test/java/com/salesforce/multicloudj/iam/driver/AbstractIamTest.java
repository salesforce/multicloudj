package com.salesforce.multicloudj.iam.driver;

import com.salesforce.multicloudj.iam.client.TestIam;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class AbstractIamTest {

    // Test constants
    private static final String TEST_PROVIDER_ID = "test";
    private static final String TEST_REGION = "testRegion";
    private static final String TEST_ROLE = "TestRole";
    private static final String TEST_ROLE_DESCRIPTION = "Test role description";
    private static final String TEST_TENANT_ID = "123456789012";
    private static final String TEST_POLICY_NAME = "TestPolicy";
    private static final String TEST_RESOURCE = "test-resource";
    private static final String TEST_IDENTITY_ID = "test-identity-id";
    private static final String TEST_IDENTITY_ARN = "test-identity-arn";
    private static final String POLICY_RESPONSE = "policy-details";
    private static final URI TEST_ENDPOINT = URI.create("https://test.endpoint.com");

    private AbstractIam mockIam;

    @BeforeEach
    void setup() {
        TestIam.Builder builder = new TestIam.Builder();
        TestIam iam = builder
                .providerId(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .withEndpoint(TEST_ENDPOINT)
                .build();
        mockIam = spy(iam);
        doCallRealMethod().when(mockIam).createIdentity(anyString(), anyString(), anyString(), anyString(), any(), any());
        doCallRealMethod().when(mockIam).attachInlinePolicy(any(), anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).getInlinePolicyDetails(anyString(), anyString(), anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).getAttachedPolicies(anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).removePolicy(anyString(), anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).deleteIdentity(anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).getIdentity(anyString(), anyString(), anyString());
        doReturn(TEST_PROVIDER_ID).when(mockIam).getProviderId();
    }

    @Test
    void testBuilder() {
        AbstractIam.Builder<TestIam, TestIam.Builder> builder = new TestIam.Builder();
        AbstractIam iam = builder
                .providerId(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .withEndpoint(TEST_ENDPOINT)
                .build();

        assertEquals(TEST_PROVIDER_ID, iam.getProviderId());
        assertEquals(TEST_REGION, iam.region);
    }

    @Test
    void testCreateIdentity() {
        doReturn(TEST_IDENTITY_ID).when(mockIam).createIdentity(
                anyString(), anyString(), anyString(), anyString(), any(), any());

        String identityId = mockIam.createIdentity(
                TEST_ROLE,
                TEST_ROLE_DESCRIPTION,
                TEST_TENANT_ID,
                TEST_REGION,
                Optional.empty(),
                Optional.empty()
        );

        verify(mockIam, times(1)).createIdentity(
                eq(TEST_ROLE),
                eq(TEST_ROLE_DESCRIPTION),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION),
                eq(Optional.empty()),
                eq(Optional.empty())
        );
        assertEquals(TEST_IDENTITY_ID, identityId);
    }

    @Test
    void testAttachInlinePolicy() {
        PolicyDocument policy = mock(PolicyDocument.class);
        mockIam.attachInlinePolicy(policy, TEST_TENANT_ID, TEST_REGION, TEST_RESOURCE);

        verify(mockIam, times(1)).attachInlinePolicy(
                eq(policy),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION),
                eq(TEST_RESOURCE)
        );
    }

    @Test
    void testGetInlinePolicyDetails() {
        // Since we're using doCallRealMethod, it will call the actual TestIam implementation
        // which returns "mock-policy-details", so we expect that value
        String policyDetails = mockIam.getInlinePolicyDetails(
                TEST_ROLE,
                TEST_POLICY_NAME,
                null,
                TEST_TENANT_ID,
                TEST_REGION
        );

        verify(mockIam, times(1)).getInlinePolicyDetails(
                eq(TEST_ROLE),
                eq(TEST_POLICY_NAME),
                eq((String)null),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION)
        );
        assertEquals("mock-policy-details", policyDetails);
    }

    @Test
    void testGetAttachedPolicies() {
        List<String> expectedPolicies = Arrays.asList("policy1", "policy2");
        doReturn(expectedPolicies).when(mockIam).getAttachedPolicies(
                anyString(), anyString(), anyString());

        List<String> policies = mockIam.getAttachedPolicies(
                TEST_ROLE,
                TEST_TENANT_ID,
                TEST_REGION
        );

        verify(mockIam, times(1)).getAttachedPolicies(
                eq(TEST_ROLE),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION)
        );
        assertNotNull(policies);
        assertEquals(2, policies.size());
        assertEquals("policy1", policies.get(0));
        assertEquals("policy2", policies.get(1));
    }

    @Test
    void testRemovePolicy() {
        mockIam.removePolicy(TEST_ROLE, TEST_POLICY_NAME, TEST_TENANT_ID, TEST_REGION);

        verify(mockIam, times(1)).removePolicy(
                eq(TEST_ROLE),
                eq(TEST_POLICY_NAME),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION)
        );
    }

    @Test
    void testDeleteIdentity() {
        mockIam.deleteIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

        verify(mockIam, times(1)).deleteIdentity(
                eq(TEST_ROLE),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION)
        );
    }

    @Test
    void testGetIdentity() {
        doReturn(TEST_IDENTITY_ARN).when(mockIam).getIdentity(
                anyString(), anyString(), anyString());

        String identityArn = mockIam.getIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);

        verify(mockIam, times(1)).getIdentity(
                eq(TEST_ROLE),
                eq(TEST_TENANT_ID),
                eq(TEST_REGION)
        );
        assertEquals(TEST_IDENTITY_ARN, identityArn);
    }

    @Test
    void testBuilderGetters() {
        AbstractIam.Builder<TestIam, TestIam.Builder> builder = new TestIam.Builder();
        builder.providerId(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .withEndpoint(TEST_ENDPOINT)
                .withCredentialsOverrider(null);

        assertEquals(TEST_REGION, builder.getRegion());
        assertEquals(TEST_ENDPOINT, builder.getEndpoint());
        assertEquals(null, builder.getCredentialsOverrider());
    }

    @Test
    void testConstructorWithBuilder() {
        TestIam.Builder builder = new TestIam.Builder()
                .providerId(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .withEndpoint(TEST_ENDPOINT);

        TestIam iam = new TestIam(builder);

        assertEquals(TEST_PROVIDER_ID, iam.getProviderId());
        assertEquals(TEST_REGION, iam.region);
    }

    @Test
    void testDirectPublicMethodDelegation() {
        TestIam testIam = new TestIam.Builder()
                .providerId(TEST_PROVIDER_ID)
                .withRegion(TEST_REGION)
                .build();

        // Test createIdentity delegation
        String identity = testIam.createIdentity(TEST_ROLE, TEST_ROLE_DESCRIPTION,
                TEST_TENANT_ID, TEST_REGION, Optional.empty(), Optional.empty());
        assertEquals("mock-identity-id", identity);

        // Test getInlinePolicyDetails delegation
        String policyDetails = testIam.getInlinePolicyDetails(TEST_ROLE, TEST_POLICY_NAME, null,
                TEST_TENANT_ID, TEST_REGION);
        assertEquals("mock-policy-details", policyDetails);

        // Test getAttachedPolicies delegation
        List<String> policies = testIam.getAttachedPolicies(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);
        assertNotNull(policies);
        assertEquals(2, policies.size());
        assertEquals("policy1", policies.get(0));
        assertEquals("policy2", policies.get(1));

        // Test getIdentity delegation
        String identityResult = testIam.getIdentity(TEST_ROLE, TEST_TENANT_ID, TEST_REGION);
        assertEquals("mock-identity-arn", identityResult);
    }
}

