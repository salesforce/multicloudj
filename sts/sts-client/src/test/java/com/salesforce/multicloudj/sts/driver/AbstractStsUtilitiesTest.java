package com.salesforce.multicloudj.sts.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractStsUtilitiesTest {

  private static class TestStsUtilities
      extends AbstractStsUtilities<AbstractStsUtilitiesTest.TestStsUtilities> {

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
    public SubstrateSdkException mapException(Throwable t) {
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
  void tearDown() {}

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

  @Test
  void endpointIsPropagatedToDriver() {
    URI endpoint = URI.create("https://sts.example.com");
    stsUtilitiesDriver.withEndpoint(endpoint);

    TestStsUtilities stsUtilities = stsUtilitiesDriver.build();

    assertEquals(endpoint, stsUtilities.endpoint);
  }

  @Test
  void driverBuilderRejectsInvalidEndpoint() {
    for (String endpoint :
        new String[] {
          "sts.example.com",
          "https:///x",
          "https://user:secret@sts.example.com",
          "ftp://sts.example.com",
          "https://sts.example.com/proxy",
          "https://sts.example.com/?a=b",
          "https://sts.example.com?a=b",
          "https://sts.example.com/#frag"
        }) {
      assertThrows(
          InvalidArgumentException.class,
          () -> stsUtilitiesDriver.withEndpoint(URI.create(endpoint)),
          endpoint);
    }
    assertThrows(InvalidArgumentException.class, () -> stsUtilitiesDriver.withEndpoint(null));
  }

  @Test
  void driverBuilderAcceptsHostWithOptionalPortAndRootPath() {
    for (String endpoint :
        new String[] {"https://sts.example.com", "HTTPS://sts.example.com/", "https://h:8443"}) {
      stsUtilitiesDriver.withEndpoint(URI.create(endpoint));
      assertEquals(URI.create(endpoint), stsUtilitiesDriver.build().endpoint, endpoint);
    }
  }
}
