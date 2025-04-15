package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

public class AbstractStsTest {

    static class TestSts extends AbstractSts<TestSts> {

        public TestSts(Builder builder) {
            super(builder);
        }

        @Override
        protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request) {
            return new StsCredentials("accessKey", "secretKey", "sessionToken");
        }

        @Override
        protected CallerIdentity getCallerIdentityFromProvider() {
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

        public static class Builder extends AbstractSts.Builder<TestSts> {
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

        CallerIdentity identity = sts.getCallerIdentity();

        assertNotNull(identity);
        assertEquals("testAccount", identity.getAccountId());
        assertEquals("testResource", identity.getCloudResourceName());
        assertEquals("testUser", identity.getUserId());
    }
}
