package com.salesforce.multicloudj.sts.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsVerifier;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.ValidateOptions;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StsVerifierTest {

  @Test
  void buildsVerifierAndValidatesSignedIdentity() {
    withMockedProvider(
        builder -> {
          StsVerifier verifier =
              builder
                  .withRegion("test-region")
                  .withEndpoint(URI.create("http://localhost:1234"))
                  .withProxyEndpoint(URI.create("http://localhost:8888"))
                  .withUseSystemPropertyProxyValues(true)
                  .withUseEnvironmentVariableProxyValues(false)
                  .build();
          assertNotNull(verifier);

          CallerIdentity identity = verifier.validateSignedAuthRequest("caller-1");
          assertEquals("caller-1", identity.getUserId());
          assertEquals("arn:test:caller-1", identity.getCloudResourceName());
          assertEquals("0", identity.getAccountId());
        });
  }

  @Test
  void passesValidateOptionsThrough() {
    withMockedProvider(
        builder -> {
          StsVerifier verifier = builder.build();
          ValidateOptions options =
              ValidateOptions.builder().withExpectedCustomHeader("x-h", "v").build();
          CallerIdentity identity = verifier.validateSignedAuthRequest("caller-2", options);
          assertEquals("1", identity.getAccountId());
        });
  }

  @Test
  void mapsExceptionThrownByVerifier() {
    withMockedProvider(
        builder -> {
          StsVerifier verifier = builder.build();
          assertThrows(
              UnknownException.class, () -> verifier.validateSignedAuthRequest("boom"));
        });
  }

  @Test
  void unknownProviderThrows() {
    withMockedProvider(
        builder ->
            assertThrows(
                IllegalArgumentException.class, () -> StsVerifier.builder("does-not-exist")));
  }

  private void withMockedProvider(Consumer<StsVerifier.StsVerifierBuilder> body) {
    TestConcreteAbstractStsVerifier provider = new TestConcreteAbstractStsVerifier();
    ServiceLoader<TestConcreteAbstractStsVerifier> serviceLoader = mock(ServiceLoader.class);
    Iterator<TestConcreteAbstractStsVerifier> providerIterator = List.of(provider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractStsVerifier.class))
          .thenReturn(serviceLoader);
      body.accept(StsVerifier.builder("mockProviderId"));
    }
  }
}
