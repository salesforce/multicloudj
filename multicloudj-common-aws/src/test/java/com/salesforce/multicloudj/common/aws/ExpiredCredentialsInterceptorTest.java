package com.salesforce.multicloudj.common.aws;

import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class ExpiredCredentialsInterceptorTest {

  private static final List<String> EXPIRED_CREDENTIALS_ERROR_CODES =
      List.of("ExpiredToken", "ExpiredTokenException", "InvalidToken", "TokenRefreshRequired");

  @Test
  public void testInvalidatesForEveryExpiredCredentialsErrorCode() {
    for (String errorCode : EXPIRED_CREDENTIALS_ERROR_CODES) {
      AtomicInteger invocations = new AtomicInteger();
      RefreshingSessionCredentialsProvider provider = refreshingProvider(invocations);
      ExpiredCredentialsInterceptor interceptor = new ExpiredCredentialsInterceptor(provider);

      Assertions.assertEquals(
          "securityToken-1", sessionToken(provider.resolveCredentials()), errorCode);

      interceptor.onExecutionFailure(
          failedExecution(serviceException(errorCode)), new ExecutionAttributes());

      Assertions.assertEquals(
          "securityToken-2", sessionToken(provider.resolveCredentials()), errorCode);
      Assertions.assertEquals(2, invocations.get(), errorCode);
    }
  }

  @Test
  public void testInvalidatesWhenTheExpiredCredentialsFailureIsWrapped() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider = refreshingProvider(invocations);
    ExpiredCredentialsInterceptor interceptor = new ExpiredCredentialsInterceptor(provider);

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    interceptor.onExecutionFailure(
        failedExecution(new CompletionException(serviceException("ExpiredToken"))),
        new ExecutionAttributes());

    Assertions.assertEquals("securityToken-2", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(2, invocations.get());
  }

  @Test
  public void testDoesNotInvalidateForAnUnrelatedErrorCode() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider = refreshingProvider(invocations);
    ExpiredCredentialsInterceptor interceptor = new ExpiredCredentialsInterceptor(provider);

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    interceptor.onExecutionFailure(
        failedExecution(serviceException("NoSuchKey")), new ExecutionAttributes());

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testDoesNotInvalidateForAFailureThatIsNotAServiceException() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider = refreshingProvider(invocations);
    ExpiredCredentialsInterceptor interceptor = new ExpiredCredentialsInterceptor(provider);

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    interceptor.onExecutionFailure(
        failedExecution(new IOException("connection reset")), new ExecutionAttributes());

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testDoesNotInvalidateForASuccessfulExecution() {
    AtomicInteger invocations = new AtomicInteger();
    RefreshingSessionCredentialsProvider provider = refreshingProvider(invocations);
    ExpiredCredentialsInterceptor interceptor = new ExpiredCredentialsInterceptor(provider);

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));

    interceptor.afterExecution(
        Mockito.mock(Context.AfterExecution.class), new ExecutionAttributes());

    Assertions.assertEquals("securityToken-1", sessionToken(provider.resolveCredentials()));
    Assertions.assertEquals(1, invocations.get());
  }

  @Test
  public void testNullProviderIsRejected() {
    Assertions.assertThrows(
        NullPointerException.class, () -> new ExpiredCredentialsInterceptor(null));
  }

  @Test
  public void testRegisterIfRefreshableSkipsANonRefreshingProvider() {
    S3ClientBuilder clientBuilder = S3Client.builder();

    ExpiredCredentialsInterceptor.registerIfRefreshable(
        clientBuilder, StaticCredentialsProvider.create(AwsBasicCredentials.create("key", "sec")));

    Assertions.assertTrue(clientBuilder.overrideConfiguration().executionInterceptors().isEmpty());
  }

  @Test
  public void testRegisterIfRefreshableSkipsANullProvider() {
    S3ClientBuilder clientBuilder = S3Client.builder();

    ExpiredCredentialsInterceptor.registerIfRefreshable(clientBuilder, null);

    Assertions.assertTrue(clientBuilder.overrideConfiguration().executionInterceptors().isEmpty());
  }

  @Test
  public void testRegisterIfRefreshablePreservesExistingOverrideConfiguration() {
    S3ClientBuilder clientBuilder = S3Client.builder();
    clientBuilder.overrideConfiguration(config -> config.apiCallTimeout(Duration.ofSeconds(7)));

    ExpiredCredentialsInterceptor.registerIfRefreshable(
        clientBuilder, refreshingProvider(new AtomicInteger()));

    ClientOverrideConfiguration overrideConfiguration = clientBuilder.overrideConfiguration();
    List<ExecutionInterceptor> interceptors = overrideConfiguration.executionInterceptors();
    Assertions.assertEquals(1, interceptors.size());
    Assertions.assertInstanceOf(ExpiredCredentialsInterceptor.class, interceptors.get(0));
    Assertions.assertEquals(
        Duration.ofSeconds(7), overrideConfiguration.apiCallTimeout().orElse(null));
  }

  /**
   * Builders mocked in the provider modules' unit tests answer {@code null} from the
   * override-configuration getter, which must not turn into a failure inside production code.
   */
  @Test
  public void testRegisterIfRefreshableToleratesABuilderWithoutOverrideConfiguration() {
    SdkClientBuilder<?, ?> clientBuilder = Mockito.mock(SdkClientBuilder.class);
    ArgumentCaptor<ClientOverrideConfiguration> registered =
        ArgumentCaptor.forClass(ClientOverrideConfiguration.class);

    ExpiredCredentialsInterceptor.registerIfRefreshable(
        clientBuilder, refreshingProvider(new AtomicInteger()));

    Mockito.verify(clientBuilder).overrideConfiguration(registered.capture());
    List<ExecutionInterceptor> interceptors = registered.getValue().executionInterceptors();
    Assertions.assertEquals(1, interceptors.size());
    Assertions.assertInstanceOf(ExpiredCredentialsInterceptor.class, interceptors.get(0));
  }

  private static RefreshingSessionCredentialsProvider refreshingProvider(
      AtomicInteger invocations) {
    return new RefreshingSessionCredentialsProvider(
        () -> {
          int invocation = invocations.incrementAndGet();
          return new StsCredentials(
              "accessKeyId-" + invocation,
              "accessKeySecret-" + invocation,
              "securityToken-" + invocation,
              Instant.now().plus(Duration.ofHours(1)));
        });
  }

  private static AwsServiceException serviceException(String errorCode) {
    return AwsServiceException.builder()
        .awsErrorDetails(AwsErrorDetails.builder().errorCode(errorCode).build())
        .build();
  }

  private static Context.FailedExecution failedExecution(Throwable exception) {
    Context.FailedExecution context = Mockito.mock(Context.FailedExecution.class);
    Mockito.when(context.exception()).thenReturn(exception);
    return context;
  }

  private static String sessionToken(AwsCredentials credentials) {
    return ((AwsSessionCredentials) credentials).sessionToken();
  }
}
