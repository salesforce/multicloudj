package com.salesforce.multicloudj.sts.ali;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.sts.client.StsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class AliStsUtilitiesTest {

  private static StsUtilities mockStsUtilities;
  private static StsCredentials credentials;
  private static String region;

  @BeforeAll
  public static void setUp() {
    mockStsUtilities = Mockito.mock(StsUtilities.class);
    region = "cn-shanghai";
    credentials = new StsCredentials("testKeyId", "testSecret", "testToken");
  }

  @Test
  public void testAliStsInitialization() {
    AliStsUtilities utilities = new AliStsUtilities();
    Assertions.assertEquals("ali", utilities.getProviderId());
  }

  @Test
  void cloudNativeAuthSignedRequest() {
    String region = "cn-shanghai";
    String service = "sts";

    // the request object we want to sign
    HttpRequest fakeRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("https://" + service + "." + region + ".aliyuncs.com"))
            .build();

    StsUtilities stsUtil =
        StsUtilities.builder("ali")
            .withRegion(region)
            .withCredentialsOverrider(
                new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(credentials)
                    .build())
            .build();

    SignedAuthRequest newSignedAuthRequest = stsUtil.newCloudNativeAuthSignedRequest(fakeRequest);
    Assertions.assertNotNull(newSignedAuthRequest.getRequest().headers());
    Assertions.assertTrue(newSignedAuthRequest.getRequest().uri().getQuery().contains("Signature"));
    Assertions.assertTrue(
        newSignedAuthRequest.getRequest().uri().getQuery().contains("SignatureNonce"));
    Assertions.assertEquals(
        credentials.getAccessKeyId(), newSignedAuthRequest.getCredentials().getAccessKeyId());
    Assertions.assertEquals(
        credentials.getAccessKeySecret(),
        newSignedAuthRequest.getCredentials().getAccessKeySecret());
    Assertions.assertEquals(
        credentials.getSecurityToken(), newSignedAuthRequest.getCredentials().getSecurityToken());
  }

  @Test
  void cloudNativeAuthSignedRequestWithNoCredentials() {
    String region = "cn-shanghai";
    String service = "sts";

    // the request object we want to sign
    HttpRequest fakeRequest =
        HttpRequest.newBuilder()
            .uri(URI.create("https://" + service + "." + region + ".aliyuncs.com"))
            .build();

    StsUtilities stsUtil = StsUtilities.builder("ali").withRegion(region).build();
    Assertions.assertThrows(
        UnAuthorizedException.class, () -> stsUtil.newCloudNativeAuthSignedRequest(fakeRequest));
  }

  @Test
  void endpointOverrideSetsSigningDomain() {
    StsUtilities stsUtil = endpointSigner("https://sts.example.com");

    SignedAuthRequest signed = stsUtil.newCloudNativeAuthSignedRequest(null);

    Assertions.assertEquals("sts.example.com", URI.create(signed.getSignedIdentity()).getHost());
    Assertions.assertEquals("sts.example.com", signed.getRequest().uri().getHost());
  }

  @Test
  void endpointOverrideWithRootPathIsAccepted() {
    SignedAuthRequest signed =
        endpointSigner("https://sts.example.com/").newCloudNativeAuthSignedRequest(null);

    Assertions.assertEquals("sts.example.com", URI.create(signed.getSignedIdentity()).getHost());
  }

  @Test
  void noEndpointKeepsRegionalDefault() {
    StsUtilities stsUtil = endpointSigner(null);

    SignedAuthRequest signed = stsUtil.newCloudNativeAuthSignedRequest(null);

    Assertions.assertEquals(
        "sts.cn-shanghai.aliyuncs.com", URI.create(signed.getSignedIdentity()).getHost());
  }

  @Test
  void endpointWithPortOrPathIsRejected() {
    for (String endpoint :
        new String[] {"https://sts.example.com:8443", "https://sts.example.com/proxy"}) {
      StsUtilities stsUtil = endpointSigner(endpoint);
      Assertions.assertThrows(
          InvalidArgumentException.class,
          () -> stsUtil.newCloudNativeAuthSignedRequest(null),
          endpoint);
    }
  }

  private static StsUtilities endpointSigner(String endpoint) {
    StsUtilities.StsUtilityBuilder builder =
        StsUtilities.builder("ali")
            .withRegion("cn-shanghai")
            .withCredentialsOverrider(
                new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(credentials)
                    .build());
    if (endpoint != null) {
      builder.withEndpoint(URI.create(endpoint));
    }
    return builder.build();
  }
}
