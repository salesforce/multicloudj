package com.salesforce.multicloudj.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class MultiCloudJLoggerFailureLoggingTest {

  private final Logger sdkLogger = (Logger) LoggerFactory.getLogger(MultiCloudJLogger.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private final OperationContext context =
      OperationContext.builder()
          .correlationId("request-123")
          .correlationIdKey("x-request-id")
          .tenantId("tenant-42")
          .serviceId("capstone")
          .build();
  private Level previousLevel;

  @BeforeEach
  void captureErrors() {
    previousLevel = sdkLogger.getLevel();
    sdkLogger.setLevel(Level.ERROR);
    appender.start();
    sdkLogger.addAppender(appender);
  }

  @AfterEach
  void stopCapturing() {
    sdkLogger.detachAppender(appender);
    appender.stop();
    sdkLogger.setLevel(previousLevel);
  }

  @Test
  void synchronousFailureIncludesIdentifiersInMessage() {
    for (TracingPolicy policy : TracingPolicy.values()) {
      appender.list.clear();
      MultiCloudJLogger logger = new MultiCloudJLogger(policy, "blob", "test");
      IllegalStateException failure = new IllegalStateException("download failed");

      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  logger.traceOperation(
                      "blob.download",
                      Map.of("bucket", "test-bucket"),
                      context,
                      ctx -> {
                        throw failure;
                      })));

      assertFailureMessage();
    }
  }

  @Test
  void asyncSupplierFailureIncludesIdentifiersInMessage() {
    for (TracingPolicy policy : TracingPolicy.values()) {
      appender.list.clear();
      MultiCloudJLogger logger = new MultiCloudJLogger(policy, "blob", "test");
      IllegalStateException failure = new IllegalStateException("download failed");

      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  logger.traceAsyncOperation(
                      "blob.download",
                      Map.of("bucket", "test-bucket"),
                      context,
                      ctx -> {
                        throw failure;
                      })));

      assertFailureMessage();
    }
  }

  @Test
  void asyncCompletionFailureIncludesIdentifiersInMessage() {
    for (TracingPolicy policy : TracingPolicy.values()) {
      appender.list.clear();
      MultiCloudJLogger logger = new MultiCloudJLogger(policy, "blob", "test");
      IllegalStateException failure = new IllegalStateException("download failed");
      CompletableFuture<String> pending = new CompletableFuture<>();

      CompletableFuture<String> result =
          logger.traceAsyncOperation(
              "blob.download", Map.of("bucket", "test-bucket"), context, ctx -> pending);
      CompletableFuture.runAsync(() -> pending.completeExceptionally(failure)).join();

      assertSame(failure, assertThrows(CompletionException.class, result::join).getCause());
      assertFailureMessage();
    }
  }

  private void assertFailureMessage() {
    assertEquals(1, appender.list.size());
    ILoggingEvent event = appender.list.get(0);
    String message = event.getFormattedMessage();
    assertTrue(message.contains("correlation_id=request-123"), message);
    assertTrue(message.contains("tenant_id=tenant-42"), message);
    assertTrue(message.contains("service_id=capstone"), message);
    assertNotNull(event.getThrowableProxy());
  }
}
