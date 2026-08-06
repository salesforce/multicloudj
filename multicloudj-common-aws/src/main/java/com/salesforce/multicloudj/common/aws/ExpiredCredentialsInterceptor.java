package com.salesforce.multicloudj.common.aws;

import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

/**
 * Invalidates a {@link RefreshingSessionCredentialsProvider} when a call fails because the
 * credentials it signed with were rejected as expired, so that subsequent calls renew instead of
 * signing with the same rejected credentials.
 *
 * <p>The call that failed is not recovered. Under the AWS SDK's SRA authentication flow an identity
 * is resolved once per API call, by an interceptor that runs outside the retry loop, so every retry
 * within a single API call re-signs with the identity resolved before the first attempt. Only the
 * next API call picks up renewed credentials.
 */
public class ExpiredCredentialsInterceptor implements ExecutionInterceptor {

  private static final Logger log = LoggerFactory.getLogger(ExpiredCredentialsInterceptor.class);

  private static final Set<String> EXPIRED_CREDENTIALS_ERROR_CODES =
      Set.of("ExpiredToken", "ExpiredTokenException", "InvalidToken", "TokenRefreshRequired");

  /** Bounds the cause walk in case an exception chain is cyclic. */
  private static final int MAX_CAUSE_DEPTH = 10;

  private final RefreshingSessionCredentialsProvider credentialsProvider;

  public ExpiredCredentialsInterceptor(RefreshingSessionCredentialsProvider credentialsProvider) {
    this.credentialsProvider =
        Objects.requireNonNull(credentialsProvider, "credentialsProvider must not be null");
  }

  /**
   * Registers an interceptor on {@code clientBuilder} when {@code credentialsProvider} supports
   * invalidation, and does nothing otherwise.
   *
   * <p>Call this after every other override-configuration change on the same builder: {@link
   * SdkClientBuilder#overrideConfiguration(java.util.function.Consumer)} builds a fresh
   * configuration and replaces whatever the builder already held, so a later call of that form
   * would drop this interceptor.
   *
   * @param clientBuilder the client builder to register on
   * @param credentialsProvider the provider the client is being built with, may be {@code null}
   */
  public static void registerIfRefreshable(
      SdkClientBuilder<?, ?> clientBuilder, AwsCredentialsProvider credentialsProvider) {
    if (!(credentialsProvider instanceof RefreshingSessionCredentialsProvider)) {
      return;
    }
    ExpiredCredentialsInterceptor interceptor =
        new ExpiredCredentialsInterceptor(
            (RefreshingSessionCredentialsProvider) credentialsProvider);
    ClientOverrideConfiguration existing = clientBuilder.overrideConfiguration();
    ClientOverrideConfiguration.Builder configuration =
        existing == null ? ClientOverrideConfiguration.builder() : existing.toBuilder();
    clientBuilder.overrideConfiguration(
        configuration.addExecutionInterceptor(interceptor).build());
  }

  @Override
  public void onExecutionFailure(
      Context.FailedExecution context, ExecutionAttributes executionAttributes) {
    String errorCode = expiredCredentialsErrorCode(context.exception());
    if (errorCode == null) {
      return;
    }
    log.info(
        "MultiCloudJ AWS call was rejected with errorCode={}; discarding the cached session"
            + " credentials so the next call renews them. The failed call is not retried with"
            + " renewed credentials.",
        errorCode);
    credentialsProvider.invalidate();
  }

  private static String expiredCredentialsErrorCode(Throwable failure) {
    Throwable cause = failure;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      if (cause instanceof AwsServiceException) {
        AwsServiceException serviceException = (AwsServiceException) cause;
        if (serviceException.awsErrorDetails() != null
            && EXPIRED_CREDENTIALS_ERROR_CODES.contains(
                serviceException.awsErrorDetails().errorCode())) {
          return serviceException.awsErrorDetails().errorCode();
        }
      }
      cause = cause.getCause();
    }
    return null;
  }
}
