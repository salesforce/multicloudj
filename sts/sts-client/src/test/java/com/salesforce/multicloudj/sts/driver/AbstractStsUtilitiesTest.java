package com.salesforce.multicloudj.sts.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.http.HttpRequest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

class AbstractStsUtilitiesTest {


    private static class TestStsUtilities extends AbstractStsUtilities<AbstractStsUtilitiesTest.TestStsUtilities> {

        public TestStsUtilities(Builder builder) {
            super(builder);
        }

        @Override
        public String getProviderId() {
            return "testProvider";
        }

        @Override
        public Provider.Builder builder() {
            return null;
        }

        @Override
        public Class<? extends SubstrateSdkException> getException(Throwable t) {
            return null;
        }

        @Override
        protected SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request) {
            return new SignedAuthRequest(request, credentials);
        }

        public static class Builder extends AbstractStsUtilities.Builder<TestStsUtilities> {
            @Override
            public TestStsUtilities build() {
                return new TestStsUtilities(this);
            }
        }
    }

    private static AbstractStsTest.TestSts.Builder stsDriver;
    private static TestStsUtilities.Builder stsUtilitiesDriver;
    private static StsCredentials credentials;
    private static CredentialsOverrider credentialsOverrider;

    @BeforeEach
    void setUp() {
        stsDriver = new AbstractStsTest.TestSts.Builder();
        stsDriver.providerId("testProvider").withRegion("testRegion");
        assertEquals("testRegion", stsDriver.getRegion());
        credentials = mock(StsCredentials.class);
        credentialsOverrider = mock(CredentialsOverrider.class);
        stsUtilitiesDriver = new TestStsUtilities.Builder();
        stsUtilitiesDriver.providerId("testProvider");
        stsUtilitiesDriver.withCredentialsOverrider(credentialsOverrider);
        assertEquals("testProvider", stsUtilitiesDriver.getProviderId());
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void newCloudNativeAuthSignedRequest() {
        AbstractStsTest.TestSts sts = stsDriver.build();
        assertEquals("testProvider", sts.getProviderId());

        TestStsUtilities stsUtilities = stsUtilitiesDriver.build();

        assertEquals("testProvider", stsUtilities.getProviderId());

        HttpRequest request = mock(HttpRequest.class);

        SignedAuthRequest signedAuthRequest = stsUtilities.newCloudNativeAuthSignedRequest(request);
        assertNotNull(signedAuthRequest.getRequest());
        assertEquals(signedAuthRequest.getCredentials(), credentials);
    }
}