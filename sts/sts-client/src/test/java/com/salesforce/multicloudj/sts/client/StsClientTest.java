package com.salesforce.multicloudj.sts.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerConfig;
import com.salesforce.multicloudj.common.circuitbreaker.CircuitBreakerExecutor;
import com.salesforce.multicloudj.common.exceptions.CircuitBreakerOpenException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StsClientTest {

  /**
   * resilience4j initializes an SLF4J logger in a static initializer, and SLF4J discovers its
   * provider through {@link ServiceLoader}. Several tests below mock {@code ServiceLoader}
   * statically; if the breaker's classes are first touched inside such a block, SLF4J's own
   * lookup returns the mock and fails to initialize. Force both static initializations here — once,
   * before any mock is installed — so the real provider is cached for the whole JVM.
   */
  @BeforeAll
  static void warmUpBreakerAndLoggingBeforeServiceLoaderMocking() {
    new CircuitBreakerExecutor("warmup", breakerConfig());
  }

  @Test
  public void testStsClient() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);

    // Mock the ServiceLoader to return mockProvider
    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    verifyServiceLoader(serviceLoader, false);
  }

  @Test
  public void testStsClientWithTestProvider() {
    TestConcreteAbstractSts provider = new TestConcreteAbstractSts();

    // Mock the ServiceLoader to return mockProvider
    ServiceLoader<TestConcreteAbstractSts> serviceLoader = mock(ServiceLoader.class);
    Iterator<TestConcreteAbstractSts> providerIterator = List.of(provider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    verifyServiceLoader(serviceLoader, true);
  }

  @Test
  public void testGetCallerIdentityWithRequest() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockProvider);

    CallerIdentity mockIdentity = new CallerIdentity("userId", "arn", "accountId");
    when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .thenReturn(mockIdentity);

    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client = StsClient.builder("mockProviderId").build();
      GetCallerIdentityRequest request =
          GetCallerIdentityRequest.builder().aud("custom-audience").build();
      CallerIdentity identity = client.getCallerIdentity(request);

      assertNotNull(identity);
      assertEquals("userId", identity.getUserId());
      assertEquals("arn", identity.getCloudResourceName());
      assertEquals("accountId", identity.getAccountId());
    }
  }

  @Test
  public void testGetAssumeRoleWithWebIdentityCredentials() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockProvider);

    StsCredentials mockCredentials = new StsCredentials("accessKey", "secretKey", "sessionToken");
    when(mockProvider.assumeRoleWithWebIdentity(any(AssumeRoleWebIdentityRequest.class)))
        .thenReturn(mockCredentials);

    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client = StsClient.builder("mockProviderId").build();
      AssumeRoleWebIdentityRequest request =
          AssumeRoleWebIdentityRequest.builder()
              .role("test-role")
              .webIdentityToken("test-token")
              .sessionName("test-session")
              .build();
      StsCredentials credentials = client.getAssumeRoleWithWebIdentityCredentials(request);

      assertNotNull(credentials);
      assertEquals("accessKey", credentials.getAccessKeyId());
      assertEquals("secretKey", credentials.getAccessKeySecret());
      assertEquals("sessionToken", credentials.getSecurityToken());
    }
  }

  @Test
  public void testGetCallerIdentityNoArgsThrowsException() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockProvider);

    when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));
    when(mockProvider.mapException(any(Throwable.class)))
        .thenAnswer(invocation -> new UnknownException((Throwable) invocation.getArgument(0)));

    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client = StsClient.builder("mockProviderId").build();
      assertThrows(SubstrateSdkException.class, () -> client.getCallerIdentity());
    }
  }

  @Test
  public void testGetCallerIdentityWithRequestThrowsException() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockProvider);

    when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));
    when(mockProvider.mapException(any(Throwable.class)))
        .thenAnswer(invocation -> new UnknownException((Throwable) invocation.getArgument(0)));

    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client = StsClient.builder("mockProviderId").build();
      GetCallerIdentityRequest request =
          GetCallerIdentityRequest.builder().aud("test-audience").build();
      assertThrows(SubstrateSdkException.class, () -> client.getCallerIdentity(request));
    }
  }

  @Test
  public void testGetAssumeRoleWithWebIdentityCredentialsThrowsException() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockProvider);

    when(mockProvider.assumeRoleWithWebIdentity(any(AssumeRoleWebIdentityRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));
    when(mockProvider.mapException(any(Throwable.class)))
        .thenAnswer(invocation -> new UnknownException((Throwable) invocation.getArgument(0)));

    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(mockProvider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client = StsClient.builder("mockProviderId").build();
      AssumeRoleWebIdentityRequest request =
          AssumeRoleWebIdentityRequest.builder()
              .role("test-role")
              .webIdentityToken("test-token")
              .sessionName("test-session")
              .build();
      assertThrows(
          SubstrateSdkException.class,
          () -> client.getAssumeRoleWithWebIdentityCredentials(request));
    }
  }

  /**
   * Deterministic breaker config: opens once five recorded calls hit a 50% retryable-failure rate.
   * The 60s window keeps every call in one bucket, so no clock manipulation is needed to observe
   * the open transition.
   */
  private static CircuitBreakerConfig breakerConfig() {
    return CircuitBreakerConfig.builder()
        .failureRateThreshold(50f)
        .slowCallRateThreshold(100f)
        .slowCallDurationThreshold(Duration.ofHours(1))
        .slidingWindowSize(60)
        .minimumNumberOfCalls(5)
        .waitDurationInOpenState(Duration.ofSeconds(10))
        .permittedNumberOfCallsInHalfOpenState(3)
        .build();
  }

  private static AbstractSts newMockProvider() {
    AbstractSts mockProvider = mock(AbstractSts.class);
    AbstractSts.Builder mockBuilder = mock(AbstractSts.Builder.class);
    when(mockProvider.getProviderId()).thenReturn("mockProviderId");
    when(mockProvider.builder()).thenReturn(mockBuilder);
    when(mockBuilder.build()).thenReturn(mockProvider);
    return mockProvider;
  }

  private static ServiceLoader<?> serviceLoaderFor(AbstractSts provider) {
    ServiceLoader serviceLoader = mock(ServiceLoader.class);
    Iterator<? extends AbstractSts> providerIterator = List.of(provider).iterator();
    when(serviceLoader.iterator()).thenReturn(providerIterator);
    return serviceLoader;
  }

  @Test
  public void circuitBreakerOpensAfterRetryableFailures() {
    AbstractSts mockProvider = newMockProvider();
    when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .thenThrow(new RuntimeException("boom"));
    // The seam maps the raw failure to a retryable SubstrateSdkException, which the breaker counts.
    when(mockProvider.mapException(any(Throwable.class)))
        .thenAnswer(inv -> new ResourceExhaustedException((Throwable) inv.getArgument(0)));

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<?> serviceLoader = serviceLoaderFor(mockProvider);

      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client =
          StsClient.builder("mockProviderId").withCircuitBreakerConfig(breakerConfig()).build();
      GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();

      // Five retryable failures reach the provider and trip the breaker.
      for (int i = 0; i < 5; i++) {
        assertThrows(ResourceExhaustedException.class, () -> client.getCallerIdentity(request));
      }

      // Breaker is now open: the call is short-circuited without touching the provider.
      assertThrows(CircuitBreakerOpenException.class, () -> client.getCallerIdentity(request));
      verify(mockProvider, times(5)).getCallerIdentity(any(GetCallerIdentityRequest.class));
    }
  }

  @Test
  public void nonRetryableFailuresDoNotOpenBreaker() {
    AbstractSts mockProvider = newMockProvider();
    when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .thenThrow(new RuntimeException("bad input"));
    // Caller errors map to a non-retryable exception; the breaker treats them as successful calls.
    when(mockProvider.mapException(any(Throwable.class)))
        .thenAnswer(inv -> new InvalidArgumentException((Throwable) inv.getArgument(0)));

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<?> serviceLoader = serviceLoaderFor(mockProvider);

      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      StsClient client =
          StsClient.builder("mockProviderId").withCircuitBreakerConfig(breakerConfig()).build();
      GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();

      // Far more than the minimum call count: the breaker never opens on non-retryable failures.
      for (int i = 0; i < 20; i++) {
        InvalidArgumentException thrown =
            assertThrows(
                InvalidArgumentException.class, () -> client.getCallerIdentity(request));
        assertFalse(thrown.isRetryable());
      }
      verify(mockProvider, times(20)).getCallerIdentity(any(GetCallerIdentityRequest.class));
    }
  }

  @Test
  public void withoutBreaker_retryableFailuresNeverShortCircuit() {
    AbstractSts mockProvider = newMockProvider();
    when(mockProvider.getCallerIdentity(any(GetCallerIdentityRequest.class)))
        .thenThrow(new RuntimeException("boom"));
    when(mockProvider.mapException(any(Throwable.class)))
        .thenAnswer(inv -> new ResourceExhaustedException((Throwable) inv.getArgument(0)));

    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      ServiceLoader<?> serviceLoader = serviceLoaderFor(mockProvider);

      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      // No breaker configured: behavior is identical to the pre-breaker client — every call reaches
      // the provider and the mapped exception propagates, never a CircuitBreakerOpenException.
      StsClient client = StsClient.builder("mockProviderId").build();
      GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();

      for (int i = 0; i < 20; i++) {
        assertThrows(ResourceExhaustedException.class, () -> client.getCallerIdentity(request));
      }
      verify(mockProvider, times(20)).getCallerIdentity(any(GetCallerIdentityRequest.class));
    }
  }

  private void verifyServiceLoader(ServiceLoader<?> serviceLoader, boolean useTestProvider) {
    try (MockedStatic<ServiceLoader> serviceLoaderStatic =
        Mockito.mockStatic(ServiceLoader.class)) {
      serviceLoaderStatic
          .when(() -> ServiceLoader.load(AbstractSts.class))
          .thenReturn(serviceLoader);

      // Execute the test logic that relies on the mocked ServiceLoader
      StsClient.StsBuilder builder = StsClient.builder("mockProviderId");
      builder.withRegion("test-region");
      builder.withEndpoint(URI.create("https://myendpoint.com"));

      StsClient client = builder.build();

      // Assertions to verify the expected behavior
      assertNotNull(client);
      if (useTestProvider) {
        assertEquals("mockProviderId", client.sts.getProviderId());
      }
    }
  }
}
