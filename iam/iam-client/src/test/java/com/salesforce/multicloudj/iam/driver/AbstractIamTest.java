package com.salesforce.multicloudj.iam.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

    static class TestIam extends AbstractIam<TestIam> {

        public TestIam(Builder builder) {
            super(builder);
        }

        @Override
        protected String doCreateIdentity(String identityName, String description, String tenantId,
                                         String region, Optional<TrustConfiguration> trustConfig,
                                         Optional<CreateOptions> options) {
            return null;
        }

        @Override
        protected void doAttachInlinePolicy(PolicyDocument policyDocument, String tenantId,
                                           String region, String resource) {
            // Mock implementation
        }

        @Override
        protected String doGetInlinePolicyDetails(String identityName, String policyName,
                                                 String tenantId, String region) {
            return null;
        }

        @Override
        protected List<String> doGetAttachedPolicies(String identityName, String tenantId, String region) {
            return null;
        }

        @Override
        protected void doRemovePolicy(String identityName, String policyName, String tenantId,
                                     String region) {
            // Mock implementation
        }

        @Override
        protected void doDeleteIdentity(String identityName, String tenantId, String region) {
            // Mock implementation
        }

        @Override
        protected String doGetIdentity(String identityName, String tenantId, String region) {
            return null;
        }

        @Override
        public Provider.Builder builder() {
            return new Builder();
        }

        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return null;
        }

        public static class Builder extends AbstractIam.Builder<TestIam, Builder> {

            protected Builder() {
                providerId("test");
            }

            @Override
            public Builder self() {
                return this;
            }

            @Override
            public TestIam build() {
                return new TestIam(this);
            }
        }
    }

    private AbstractIam<TestIam> mockIam;

    @BeforeEach
    void setup() {
        var iam = new TestIam.Builder()
                .providerId("testProvider")
                .withRegion("testRegion")
                .withEndpoint(URI.create("https://someendpoint.com"))
                .build();
        mockIam = spy(iam);
        doCallRealMethod().when(mockIam).createIdentity(anyString(), anyString(), anyString(), anyString(), any(), any());
        doCallRealMethod().when(mockIam).attachInlinePolicy(any(), anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).getInlinePolicyDetails(anyString(), anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).getAttachedPolicies(anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).removePolicy(anyString(), anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).deleteIdentity(anyString(), anyString(), anyString());
        doCallRealMethod().when(mockIam).getIdentity(anyString(), anyString(), anyString());
        doReturn("testProvider").when(mockIam).getProviderId();
    }

    @Test
    void testBuilder() {
        AbstractIam.Builder<TestIam, TestIam.Builder> builder = new TestIam.Builder();
        AbstractIam<TestIam> iam = builder
                .providerId("testProvider")
                .withRegion("testRegion")
                .withEndpoint(URI.create("https://test.example.com"))
                .build();

        assertEquals("testProvider", iam.getProviderId());
        assertEquals("testRegion", iam.region);
    }

    @Test
    void testCreateIdentity() {
        doReturn("test-identity-id").when(mockIam).createIdentity(
                anyString(), anyString(), anyString(), anyString(), any(), any());

        String identityId = mockIam.createIdentity(
                "test-role",
                "Test role description",
                "123456789012",
                "testRegion",
                Optional.empty(),
                Optional.empty()
        );

        ArgumentCaptor<String> identityNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> descriptionCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tenantIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> regionCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Optional<TrustConfiguration>> trustConfigCaptor = ArgumentCaptor.forClass(Optional.class);
        ArgumentCaptor<Optional<CreateOptions>> optionsCaptor = ArgumentCaptor.forClass(Optional.class);

        verify(mockIam, times(1)).createIdentity(
                identityNameCaptor.capture(),
                descriptionCaptor.capture(),
                tenantIdCaptor.capture(),
                regionCaptor.capture(),
                trustConfigCaptor.capture(),
                optionsCaptor.capture()
        );

        assertEquals("test-role", identityNameCaptor.getValue());
        assertEquals("Test role description", descriptionCaptor.getValue());
        assertEquals("123456789012", tenantIdCaptor.getValue());
        assertEquals("testRegion", regionCaptor.getValue());
        assertEquals(Optional.empty(), trustConfigCaptor.getValue());
        assertEquals(Optional.empty(), optionsCaptor.getValue());
        assertEquals("test-identity-id", identityId);
    }

    @Test
    void testAttachInlinePolicy() {
        PolicyDocument policy = mock(PolicyDocument.class);
        mockIam.attachInlinePolicy(policy, "123456789012", "testRegion", "test-resource");

        ArgumentCaptor<PolicyDocument> policyCaptor = ArgumentCaptor.forClass(PolicyDocument.class);
        ArgumentCaptor<String> tenantIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> regionCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> resourceCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockIam, times(1)).attachInlinePolicy(
                policyCaptor.capture(),
                tenantIdCaptor.capture(),
                regionCaptor.capture(),
                resourceCaptor.capture()
        );

        assertEquals(policy, policyCaptor.getValue());
        assertEquals("123456789012", tenantIdCaptor.getValue());
        assertEquals("testRegion", regionCaptor.getValue());
        assertEquals("test-resource", resourceCaptor.getValue());
    }

    @Test
    void testGetInlinePolicyDetails() {
        doReturn("test-policy-document").when(mockIam).getInlinePolicyDetails(
                anyString(), anyString(), anyString(), anyString());

        String policyDetails = mockIam.getInlinePolicyDetails(
                "test-role",
                "test-policy",
                "123456789012",
                "testRegion"
        );

        ArgumentCaptor<String> identityNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> policyNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tenantIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> regionCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockIam, times(1)).getInlinePolicyDetails(
                identityNameCaptor.capture(),
                policyNameCaptor.capture(),
                tenantIdCaptor.capture(),
                regionCaptor.capture()
        );

        assertEquals("test-role", identityNameCaptor.getValue());
        assertEquals("test-policy", policyNameCaptor.getValue());
        assertEquals("123456789012", tenantIdCaptor.getValue());
        assertEquals("testRegion", regionCaptor.getValue());
        assertEquals("test-policy-document", policyDetails);
    }

    @Test
    void testGetAttachedPolicies() {
        List<String> expectedPolicies = Arrays.asList("policy1", "policy2");
        doReturn(expectedPolicies).when(mockIam).getAttachedPolicies(
                anyString(), anyString(), anyString());

        List<String> policies = mockIam.getAttachedPolicies(
                "test-role",
                "123456789012",
                "testRegion"
        );

        ArgumentCaptor<String> identityNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tenantIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> regionCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockIam, times(1)).getAttachedPolicies(
                identityNameCaptor.capture(),
                tenantIdCaptor.capture(),
                regionCaptor.capture()
        );

        assertEquals("test-role", identityNameCaptor.getValue());
        assertEquals("123456789012", tenantIdCaptor.getValue());
        assertEquals("testRegion", regionCaptor.getValue());
        assertNotNull(policies);
        assertEquals(2, policies.size());
        assertEquals("policy1", policies.get(0));
        assertEquals("policy2", policies.get(1));
    }

    @Test
    void testRemovePolicy() {
        mockIam.removePolicy("test-role", "test-policy", "123456789012", "testRegion");

        verify(mockIam, times(1)).removePolicy(
                eq("test-role"),
                eq("test-policy"),
                eq("123456789012"),
                eq("testRegion")
        );
    }

    @Test
    void testDeleteIdentity() {
        mockIam.deleteIdentity("test-role", "123456789012", "testRegion");

        verify(mockIam, times(1)).deleteIdentity(
                eq("test-role"),
                eq("123456789012"),
                eq("testRegion")
        );
    }

    @Test
    void testGetIdentity() {
        doReturn("test-identity-arn").when(mockIam).getIdentity(
                anyString(), anyString(), anyString());

        String identityArn = mockIam.getIdentity("test-role", "123456789012", "testRegion");

        ArgumentCaptor<String> identityNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> tenantIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> regionCaptor = ArgumentCaptor.forClass(String.class);

        verify(mockIam, times(1)).getIdentity(
                identityNameCaptor.capture(),
                tenantIdCaptor.capture(),
                regionCaptor.capture()
        );

        assertEquals("test-role", identityNameCaptor.getValue());
        assertEquals("123456789012", tenantIdCaptor.getValue());
        assertEquals("testRegion", regionCaptor.getValue());
        assertEquals("test-identity-arn", identityArn);
    }

    @Test
    void testBuilderGetters() {
        AbstractIam.Builder<TestIam, TestIam.Builder> builder = new TestIam.Builder();
        builder.providerId("testProvider")
                .withRegion("testRegion")
                .withEndpoint(URI.create("https://test.example.com"))
                .withCredentialsOverrider(null);

        assertEquals("testRegion", builder.getRegion());
        assertEquals(URI.create("https://test.example.com"), builder.getEndpoint());
        assertEquals(null, builder.getCredentialsOverrider());
    }

    @Test
    void testConstructorWithBuilder() {
        TestIam.Builder builder = new TestIam.Builder();
        builder.providerId("testProvider2")
                .withRegion("testRegion")
                .withEndpoint(URI.create("https://custom.endpoint.com"));

        TestIam iam = new TestIam(builder);

        assertEquals("testProvider2", iam.getProviderId());
        assertEquals("testRegion", iam.region);
    }
}

