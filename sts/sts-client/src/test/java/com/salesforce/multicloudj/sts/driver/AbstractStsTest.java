package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.CredentialScope;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public class AbstractStsTest {

    static class TestSts extends AbstractSts {

        public TestSts(Builder builder) {
            super(builder);
        }

        @Override
        protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request) {
            return new StsCredentials("accessKey", "secretKey", "sessionToken");
        }

        @Override
        protected StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest request) {
            return new StsCredentials("accessKey", "secretKey", "sessionToken");
        }

        @Override
        protected CallerIdentity getCallerIdentityFromProvider(GetCallerIdentityRequest request) {
            return new CallerIdentity("testUser", "testResource", "testAccount");
        }

        @Override
        protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
            return new StsCredentials("accessKey", "secretKey", "sessionToken");
        }

        @Override
        public Provider.Builder builder() {
            return null;
        }

        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return null;
        }

        public static class Builder extends AbstractSts.Builder<TestSts, Builder> {
            @Override
            public Builder self() {
                return this;
            }

            @Override
            public TestSts build() {
                return new TestSts(this);
            }
        }
    }

    private static TestSts.Builder builder;

    @BeforeAll
    public static void setUp() {
        builder = new TestSts.Builder();
        builder.providerId("testProvider").withRegion("testRegion").withEndpoint(URI.create("https://someendpoint.com"));
        assertEquals("testRegion", builder.getRegion());
        assertEquals(URI.create("https://someendpoint.com"), builder.getEndpoint());
    }

    @Test
    public void testAssumeRole() {
        TestSts sts = builder.build();
        assertEquals("testProvider", sts.getProviderId());

        AssumedRoleRequest request = mock(AssumedRoleRequest.class);
        StsCredentials credentials = sts.assumeRole(request);

        assertNotNull(credentials);
        assertEquals("accessKey", credentials.getAccessKeyId());
        assertEquals("secretKey", credentials.getAccessKeySecret());
        assertEquals("sessionToken", credentials.getSecurityToken());
    }

    @Test
    public void testGetSessionToken() {
        TestSts sts = builder.build();
        assertEquals("testProvider", sts.getProviderId());

        GetAccessTokenRequest request = mock(GetAccessTokenRequest.class);
        StsCredentials credentials = sts.getAccessToken(request);

        assertNotNull(credentials);
        assertEquals("accessKey", credentials.getAccessKeyId());
        assertEquals("secretKey", credentials.getAccessKeySecret());
        assertEquals("sessionToken", credentials.getSecurityToken());
    }

    @Test
    public void testGetCallerIdentity() {
        TestSts sts = builder.build();
        assertEquals("testProvider", sts.getProviderId());

        CallerIdentity identity = sts.getCallerIdentity(GetCallerIdentityRequest.builder().build());

        assertNotNull(identity);
        assertEquals("testAccount", identity.getAccountId());
        assertEquals("testResource", identity.getCloudResourceName());
        assertEquals("testUser", identity.getUserId());
    }

    @Test
    public void testValidCredentialScope() {
        TestSts sts = builder.build();

        // Valid storage-only credential scope
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://my-bucket")
                .availablePermission("storage:GetObject")
                .availablePermission("storage:PutObject")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("test-role")
                .withCredentialScope(credentialScope)
                .build();

        // Should not throw
        StsCredentials credentials = sts.assumeRole(request);
        assertNotNull(credentials);
    }

    @Test
    public void testInvalidCredentialScopeWithNonStorageResource() {
        TestSts sts = builder.build();

        // Invalid: non-storage resource
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("compute://my-instance")
                .availablePermission("storage:GetObject")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("test-role")
                .withCredentialScope(credentialScope)
                .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            sts.assumeRole(request);
        });
        assertEquals("Credential scope resource must start with 'storage://'. Found: compute://my-instance",
                exception.getMessage());
    }

    @Test
    public void testInvalidCredentialScopeWithNonStoragePermission() {
        TestSts sts = builder.build();

        // Invalid: non-storage permission
        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://my-bucket")
                .availablePermission("compute:StartInstance")
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("test-role")
                .withCredentialScope(credentialScope)
                .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            sts.assumeRole(request);
        });
        assertEquals("Credential scope permission must start with 'storage:'. Found: compute:StartInstance",
                exception.getMessage());
    }

    @Test
    public void testInvalidCredentialScopeWithNonStorageConditionPrefix() {
        TestSts sts = builder.build();

        // Invalid: non-storage condition resourcePrefix
        CredentialScope.AvailabilityCondition condition = CredentialScope.AvailabilityCondition.builder()
                .resourcePrefix("compute://my-instance/logs/")
                .build();

        CredentialScope.ScopeRule rule = CredentialScope.ScopeRule.builder()
                .availableResource("storage://my-bucket")
                .availablePermission("storage:GetObject")
                .availabilityCondition(condition)
                .build();

        CredentialScope credentialScope = CredentialScope.builder()
                .rule(rule)
                .build();

        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("test-role")
                .withCredentialScope(credentialScope)
                .build();

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            sts.assumeRole(request);
        });
        assertEquals("Credential scope condition resourcePrefix must start with 'storage://'. Found: compute://my-instance/logs/",
                exception.getMessage());
    }

    @Test
    public void testNullCredentialScopeIsValid() {
        TestSts sts = builder.build();

        // Null credential scope is valid
        AssumedRoleRequest request = AssumedRoleRequest.newBuilder()
                .withRole("test-role")
                .build();

        // Should not throw
        StsCredentials credentials = sts.assumeRole(request);
        assertNotNull(credentials);
    }
}
