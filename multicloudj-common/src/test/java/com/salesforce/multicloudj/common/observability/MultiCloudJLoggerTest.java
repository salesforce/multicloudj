package com.salesforce.multicloudj.common.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.MDC;

class MultiCloudJLoggerTest {

  @RegisterExtension
  static final OpenTelemetryExtension otel = OpenTelemetryExtension.create();

  @BeforeEach
  @AfterEach
  void resetState() {
    MDC.clear();
    MultiCloudJObservability.setDefaultTracingPolicy(TracingPolicy.DISABLED);
  }

  // --- DISABLED policy -----------------------------------------------------

  @Test
  void disabled_noSpanCreated_butMdcSet() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "test");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();
    AtomicReference<String> capturedTraceId = new AtomicReference<>();

    String result =
        logger.traceOperation(
            "blob.test",
            Map.of("bucket", "b1"),
            null,
            ctx -> {
              capturedCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
              capturedTraceId.set(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
              return "ok";
            });

    assertEquals("ok", result);
    assertNotNull(capturedCorrelation.get());
    assertNull(capturedTraceId.get(), "trace_id MDC should not be set under DISABLED");
    assertTrue(otel.getSpans().isEmpty(), "no spans expected under DISABLED");
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID), "MDC must be cleared after");
  }

  // --- JOIN_ONLY policy ----------------------------------------------------

  @Test
  void joinOnly_noParent_noSpanCreated_butMdcSet() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.JOIN_ONLY, "blob", "test");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();
    AtomicReference<String> capturedTraceId = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          capturedCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
          capturedTraceId.set(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
          return null;
        });

    assertNotNull(capturedCorrelation.get());
    assertNull(capturedTraceId.get(), "trace_id MDC should not be set when JOIN_ONLY skips span");
    assertTrue(otel.getSpans().isEmpty());
  }

  @Test
  void joinOnly_withParent_childSpanCreated() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.JOIN_ONLY, "blob", "test");
    Span parent = otel.getOpenTelemetry().getTracer("test").spanBuilder("parent").startSpan();
    String parentTraceId = parent.getSpanContext().getTraceId();

    try (Scope ignored = parent.makeCurrent()) {
      logger.traceOperation("blob.test", Map.of("bucket", "b1"), null, ctx -> null);
    } finally {
      parent.end();
    }

    List<SpanData> spans = otel.getSpans();
    assertEquals(2, spans.size(), "parent + child");
    SpanData child =
        spans.stream().filter(s -> "blob.test".equals(s.getName())).findFirst().orElseThrow();
    assertEquals(parentTraceId, child.getTraceId(), "child must share parent trace");
    assertNotNull(child.getParentSpanContext().getSpanId());
  }

  // --- CHILD_AND_ROOT policy -----------------------------------------------

  @Test
  void childAndRoot_noParent_rootSpanCreated() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    logger.traceOperation("blob.upload", Map.of("bucket", "my-bucket"), null, ctx -> null);

    List<SpanData> spans = otel.getSpans();
    assertEquals(1, spans.size());
    SpanData span = spans.get(0);
    assertEquals("blob.upload", span.getName());
    assertFalse(span.getParentSpanContext().isValid(), "root span has no parent");
    assertEquals("my-bucket", span.getAttributes().get(AttributeKey.stringKey("bucket")));
    assertEquals("blob", span.getAttributes().get(AttributeKey.stringKey("sdk_service")));
    assertEquals("aws", span.getAttributes().get(AttributeKey.stringKey("sdk_provider")));
    assertNotNull(span.getAttributes().get(AttributeKey.stringKey("correlation_id")));
  }

  @Test
  void childAndRoot_withParent_childSpanCreated() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "gcp");
    Span parent = otel.getOpenTelemetry().getTracer("test").spanBuilder("parent").startSpan();
    String parentTraceId = parent.getSpanContext().getTraceId();

    try (Scope ignored = parent.makeCurrent()) {
      logger.traceOperation("blob.test", null, null, ctx -> null);
    } finally {
      parent.end();
    }

    SpanData child =
        otel.getSpans().stream()
            .filter(s -> "blob.test".equals(s.getName()))
            .findFirst()
            .orElseThrow();
    assertEquals(parentTraceId, child.getTraceId());
  }

  // --- Correlation ID flow -------------------------------------------------

  @Test
  void correlationId_provided_echoedInResolvedContext() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    OperationContext input = OperationContext.builder().correlationId("req-abc-123").build();
    AtomicReference<OperationContext> resolved = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        input,
        ctx -> {
          resolved.set(ctx);
          return null;
        });

    assertEquals("req-abc-123", resolved.get().getCorrelationId());
    assertEquals(
        "req-abc-123",
        otel.getSpans().get(0).getAttributes().get(AttributeKey.stringKey("correlation_id")));
  }

  @Test
  void correlationId_null_generatedAsUuid() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<OperationContext> resolved = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          resolved.set(ctx);
          return null;
        });

    String generated = resolved.get().getCorrelationId();
    assertNotNull(generated);
    assertEquals(
        generated,
        UUID.fromString(generated).toString(),
        "auto-generated correlation ID must be a valid UUID");
  }

  @Test
  void correlationId_emptyString_treatedAsMissingAndGenerated() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "aws");
    OperationContext input = OperationContext.builder().correlationId("").build();
    AtomicReference<OperationContext> resolved = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        input,
        ctx -> {
          resolved.set(ctx);
          return null;
        });

    String generated = resolved.get().getCorrelationId();
    assertNotNull(generated);
    assertFalse(generated.isEmpty());
  }

  // --- MDC --------------------------------------------------------------

  @Test
  void mdcPopulated_during_clearedAfter() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<String> capturedTraceId = new AtomicReference<>();
    AtomicReference<String> capturedSpanId = new AtomicReference<>();
    AtomicReference<String> capturedSdkService = new AtomicReference<>();
    AtomicReference<String> capturedSdkProvider = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          capturedTraceId.set(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
          capturedSpanId.set(MDC.get(MultiCloudJLogger.MDC_SPAN_ID));
          capturedSdkService.set(MDC.get(MultiCloudJLogger.MDC_SDK_SERVICE));
          capturedSdkProvider.set(MDC.get(MultiCloudJLogger.MDC_SDK_PROVIDER));
          return null;
        });

    assertNotNull(capturedTraceId.get());
    assertNotNull(capturedSpanId.get());
    assertEquals("blob", capturedSdkService.get());
    assertEquals("aws", capturedSdkProvider.get());

    assertNull(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
    assertNull(MDC.get(MultiCloudJLogger.MDC_SPAN_ID));
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
    assertNull(MDC.get(MultiCloudJLogger.MDC_SDK_SERVICE));
    assertNull(MDC.get(MultiCloudJLogger.MDC_SDK_PROVIDER));
  }

  @Test
  void mdcCleared_evenOnException() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    assertThrows(
        IllegalStateException.class,
        () ->
            logger.traceOperation(
                "blob.test",
                null,
                null,
                ctx -> {
                  throw new IllegalStateException("boom");
                }));

    assertNull(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
  }

  // --- Exception recording --------------------------------------------------

  @Test
  void spanRecordsException_andSetsErrorStatus() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    IllegalStateException boom = new IllegalStateException("boom");

    assertThrows(
        IllegalStateException.class,
        () ->
            logger.traceOperation(
                "blob.test",
                null,
                null,
                ctx -> {
                  throw boom;
                }));

    SpanData span = otel.getSpans().get(0);
    assertSame(StatusCode.ERROR, span.getStatus().getStatusCode());
    assertEquals("boom", span.getStatus().getDescription());
    assertFalse(span.getEvents().isEmpty(), "exception event recorded");
  }

  @Test
  void disabledPolicy_exceptionStillPropagates_noSpan() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "aws");

    assertThrows(
        RuntimeException.class,
        () ->
            logger.traceOperation(
                "blob.test",
                null,
                null,
                ctx -> {
                  throw new RuntimeException("nope");
                }));

    assertTrue(otel.getSpans().isEmpty());
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
  }

  // --- Global default fallback ---------------------------------------------

  @Test
  void nullPolicy_fallsBackToGlobalDefault() {
    MultiCloudJObservability.setDefaultTracingPolicy(TracingPolicy.CHILD_AND_ROOT);
    MultiCloudJLogger logger = new MultiCloudJLogger(null, "blob", "aws");

    logger.traceOperation("blob.test", null, null, ctx -> null);

    assertEquals(1, otel.getSpans().size());
  }

  // --- traceVoidOperation --------------------------------------------------

  @Test
  void traceVoidOperation_runsAndCreatesSpan() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<OperationContext> seen = new AtomicReference<>();

    logger.traceVoidOperation("blob.delete", Map.of("bucket", "b1"), null, seen::set);

    assertNotNull(seen.get());
    assertEquals(1, otel.getSpans().size());
    assertEquals("blob.delete", otel.getSpans().get(0).getName());
  }

  // --- traceAsyncOperation -------------------------------------------------

  @Test
  void traceAsyncOperation_succeeds_spanEndedAfterFutureCompletes()
      throws ExecutionException, InterruptedException {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    CompletableFuture<String> future =
        logger.traceAsyncOperation(
            "blob.upload.async",
            Map.of("bucket", "b1"),
            null,
            ctx -> CompletableFuture.completedFuture("ok-" + ctx.getCorrelationId()));

    String result = future.get();
    assertTrue(result.startsWith("ok-"));
    assertEquals(1, otel.getSpans().size());
    SpanData span = otel.getSpans().get(0);
    assertEquals("blob.upload.async", span.getName());
    assertSame(StatusCode.UNSET, span.getStatus().getStatusCode());
  }

  @Test
  void traceAsyncOperation_failedFuture_recordsExceptionAndPropagates() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    IllegalStateException boom = new IllegalStateException("async-boom");
    CompletableFuture<String> failing = new CompletableFuture<>();
    failing.completeExceptionally(boom);

    CompletableFuture<String> future =
        logger.traceAsyncOperation("blob.test.async", null, null, ctx -> failing);

    ExecutionException thrown =
        assertThrows(ExecutionException.class, future::get);
    Throwable rootCause =
        thrown.getCause() instanceof CompletionException
            ? thrown.getCause().getCause()
            : thrown.getCause();
    assertEquals("async-boom", rootCause.getMessage());

    SpanData span = otel.getSpans().get(0);
    assertSame(StatusCode.ERROR, span.getStatus().getStatusCode());
    assertFalse(span.getEvents().isEmpty());
  }

  @Test
  void traceAsyncOperation_disabled_noSpanButMdcSet()
      throws ExecutionException, InterruptedException {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "aws");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();

    CompletableFuture<String> future =
        logger.traceAsyncOperation(
            "blob.test.async",
            null,
            null,
            ctx -> {
              capturedCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
              return CompletableFuture.completedFuture("ok");
            });

    assertEquals("ok", future.get());
    assertNotNull(capturedCorrelation.get());
    assertTrue(otel.getSpans().isEmpty());
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
  }

  @Test
  void traceAsyncOperation_supplierThrowsSync_endsSpanAndRethrows() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    assertThrows(
        IllegalStateException.class,
        () ->
            logger.traceAsyncOperation(
                "blob.test.async",
                null,
                null,
                ctx -> {
                  throw new IllegalStateException("sync-boom");
                }));

    assertEquals(1, otel.getSpans().size());
    assertSame(StatusCode.ERROR, otel.getSpans().get(0).getStatus().getStatusCode());
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
  }
}
