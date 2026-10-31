package com.salesforce.multicloudj.sts.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class StsUtilitiesTest {

  @Test
  public void testStsUtilities() {
    AbstractStsUtilities<?> mockProvider = mock(AbstractStsUtilities.class);
    AbstractStsUtilities.Builder<?> mockBuilder = mock(AbstractStsUtilities.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);

    // Mock the ServiceLoader to return mockProvider
    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractStsUtilities<?>> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    verifyServiceLoader(serviceLoader, false);
  }

  @Test
  public void testStsUtilitiesWithTestProvider() {
    TestConcreteAbstractStsUtilities provider = new TestConcreteAbstractStsUtilities();

    // Mock the ServiceLoader to return mockProvider
    ServiceLoader<TestConcreteAbstractStsUtilities> serviceLoader = mock(ServiceLoader.class);
    Iterator<TestConcreteAbstractStsUtilities> providerIterator = List.of(provider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    verifyServiceLoader(serviceLoader, true);
  }

  @Test
  public void withEndpointRejectsRelativeUri() {
    TestConcreteAbstractStsUtilities provider = new TestConcreteAbstractStsUtilities();
    ServiceLoader<TestConcreteAbstractStsUtilities> serviceLoader = mock(ServiceLoader.class);
    when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractStsUtilities.class))
          .thenReturn(serviceLoader);
      StsUtilities.StsUtilityBuilder builder = StsUtilities.builder("mockProviderId");

      assertThrows(
          InvalidArgumentException.class,
          () -> builder.withEndpoint(URI.create("sts.example.com")));
      assertThrows(InvalidArgumentException.class, () -> builder.withEndpoint(null));
      assertThrows(
          InvalidArgumentException.class, () -> builder.withEndpoint(URI.create("https:///x")));
    }
  }

  @Test
  public void withEndpointRejectsUserInfoAndNonHttpScheme() {
    TestConcreteAbstractStsUtilities provider = new TestConcreteAbstractStsUtilities();
    ServiceLoader<TestConcreteAbstractStsUtilities> serviceLoader = mock(ServiceLoader.class);
    when(serviceLoader.iterator()).thenReturn(List.of(provider).iterator());

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractStsUtilities.class))
          .thenReturn(serviceLoader);
      StsUtilities.StsUtilityBuilder builder = StsUtilities.builder("mockProviderId");

      assertThrows(
          InvalidArgumentException.class,
          () -> builder.withEndpoint(URI.create("https://user:secret@sts.example.com")));
      assertThrows(
          InvalidArgumentException.class, () -> builder.withEndpoint(URI.create("ftp://h")));
      assertSame(builder, builder.withEndpoint(URI.create("HTTPS://sts.example.com")));
      assertSame(builder, builder.withEndpoint(URI.create("http://localhost:8080")));
    }
  }

  private void verifyServiceLoader(ServiceLoader<?> serviceLoader, boolean useTestProvider) {
    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractStsUtilities.class))
          .thenReturn(serviceLoader);

      CredentialsOverrider mockCreds = mock(CredentialsOverrider.class);
      // Execute the test logic that relies on the mocked ServiceLoader
      StsUtilities.StsUtilityBuilder builder = StsUtilities.builder("mockProviderId");
      builder.withRegion("test-region");
      builder.withCredentialsOverrider(mockCreds);
      StsUtilities client = builder.build();

      // Assertions to verify the expected behavior
      assertNotNull(client);
      if (useTestProvider) {
        assertEquals("mockProviderId", client.stsUtility.getProviderId());
      }
    }
  }
}
