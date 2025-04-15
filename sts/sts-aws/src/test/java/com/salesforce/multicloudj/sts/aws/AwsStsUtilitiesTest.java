package com.salesforce.multicloudj.sts.aws;

import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.net.http.HttpRequest;

class AwsStsUtilitiesTest {

    private static StsUtilities mockStsUtilities;
    private static StsCredentials credentials;
    private static String region;

    @BeforeAll
    public static void setUp() {
        mockStsUtilities = Mockito.mock(StsUtilities.class);
        region = "us-west-2";
        credentials = new StsCredentials(
                "testKeyId",
                "testSecret",
                "testToken"
        );
    }

    @Test
    public void TestAwsStsInitialization() {
        AwsStsUtilities utilities = new AwsStsUtilities();
        Assertions.assertEquals("aws", utilities.getProviderId());
    }

    @Test
    void cloudNativeAuthSignedRequest() {
        String region = "us-west-2";
        String service = "sts";
        String apiName = "GetCallerIdentity";
        String apiVersion = "2011-06-15";

        // create a sample bodypublisher
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofString("Action=" + apiName + "&Version=" + apiVersion);

        // the request object we want to sign
        HttpRequest fakeRequest = HttpRequest.newBuilder()
                .POST(body)
                .uri(URI.create("https://" + service + "." + region + ".amazonaws.com:443"))
                .build();

        StsUtilities stsUtil = StsUtilities.builder("aws")
                .withRegion(region)
                .withCredentialsOverrider(new CredentialsOverrider.Builder(CredentialsType.SESSION)
                        .withSessionCredentials(credentials).build())
                .build();

        SignedAuthRequest newSignedAuthRequest = stsUtil.newCloudNativeAuthSignedRequest(fakeRequest);

        Assertions.assertEquals(credentials.getAccessKeyId(), newSignedAuthRequest.getCredentials().getAccessKeyId());
        Assertions.assertEquals(credentials.getAccessKeySecret(), newSignedAuthRequest.getCredentials().getAccessKeySecret());
        Assertions.assertEquals(credentials.getSecurityToken(), newSignedAuthRequest.getCredentials().getSecurityToken());
    }

}