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
  void disabled_noSpanCreated_butCorrelationIdInMdc() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "test");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();
    AtomicReference<String> capturedTraceId = new AtomicReference<>();

    String result =
        logger.traceOperation(
            "blob.test",
            Map.of("bucket", "b1"),
            OperationContext.builder().build(),
            ctx -> {
              capturedCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
              capturedTraceId.set(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
              return "ok";
            });

    assertEquals("ok", result);
    assertNotNull(capturedCorrelation.get(), "SDK-generated id must appear under the SDK key");
    assertNull(capturedTraceId.get(), "trace_id MDC should not be set under DISABLED");
    assertTrue(otel.getSpans().isEmpty(), "no spans expected under DISABLED");
    assertNull(
        MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID),
        "SDK key in MDC must be cleared after");
  }

  @Test
  void disabled_nullContext_correlationIdStillGenerated() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "test");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          capturedCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
          return null;
        });

    assertNotNull(
        capturedCorrelation.get(),
        "SDK always generates a correlation id even with null context");
  }

  // --- JOIN_ONLY policy ----------------------------------------------------

  @Test
  void joinOnly_noParent_noSpanCreated_correlationStillInMdc() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.JOIN_ONLY, "blob", "test");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();
    AtomicReference<String> capturedTraceId = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        OperationContext.builder().build(),
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
  void childAndRoot_noParent_rootSpanCreated_withCorrelationAttribute() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    logger.traceOperation(
        "blob.upload", Map.of("bucket", "my-bucket"),
        OperationContext.builder().build(), ctx -> null);

    List<SpanData> spans = otel.getSpans();
    assertEquals(1, spans.size());
    SpanData span = spans.get(0);
    assertEquals("blob.upload", span.getName());
    assertFalse(span.getParentSpanContext().isValid(), "root span has no parent");
    assertEquals("my-bucket", span.getAttributes().get(AttributeKey.stringKey("bucket")));
    assertEquals("blob", span.getAttributes().get(AttributeKey.stringKey("sdk_service")));
    assertEquals("aws", span.getAttributes().get(AttributeKey.stringKey("sdk_provider")));
    assertNotNull(
        span.getAttributes().get(AttributeKey.stringKey(
            MultiCloudJLogger.CORRELATION_ID_METADATA_KEY)),
        "correlation attribute must use the SDK's well-known key");
  }

  @Test
  void childAndRoot_nullContext_correlationAttributeStillPresent() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    logger.traceOperation("blob.test", null, null, ctx -> null);

    SpanData span = otel.getSpans().get(0);
    assertEquals("blob", span.getAttributes().get(AttributeKey.stringKey("sdk_service")));
    assertNotNull(
        span.getAttributes().get(AttributeKey.stringKey(
            MultiCloudJLogger.CORRELATION_ID_METADATA_KEY)),
        "SDK always stamps correlation attribute even with null context");
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
  void correlationId_provided_echoedAndSurfaced() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<OperationContext> resolved = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        OperationContext.builder().correlationId("req-abc-123").build(),
        ctx -> {
          resolved.set(ctx);
          return null;
        });

    assertEquals("req-abc-123", resolved.get().getCorrelationId());
    assertEquals(
        "req-abc-123",
        otel.getSpans().get(0).getAttributes().get(
            AttributeKey.stringKey(MultiCloudJLogger.CORRELATION_ID_METADATA_KEY)));
  }

  @Test
  void correlationId_mdcAlreadyHasValue_sdkReadsItFromMdc() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    MDC.put(MultiCloudJLogger.MDC_CORRELATION_ID, "from-mdc-7f31");
    AtomicReference<OperationContext> resolved = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        OperationContext.builder().build(),
        ctx -> {
          resolved.set(ctx);
          return null;
        });

    assertEquals(
        "from-mdc-7f31",
        resolved.get().getCorrelationId(),
        "SDK must reuse the MDC value the caller's request filter already populated");
    assertEquals(
        "from-mdc-7f31",
        otel.getSpans().get(0).getAttributes().get(
            AttributeKey.stringKey(MultiCloudJLogger.CORRELATION_ID_METADATA_KEY)));
  }

  @Test
  void correlationId_noValue_noMdc_sdkGeneratesUuid() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<OperationContext> resolved = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        OperationContext.builder().build(),
        ctx -> {
          resolved.set(ctx);
          return null;
        });

    String generated = resolved.get().getCorrelationId();
    assertNotNull(generated);
    assertEquals(
        generated,
        UUID.fromString(generated).toString(),
        "auto-generated correlation id must be a valid UUID");
  }

  @Test
  void correlationId_emptyExplicitValue_treatedAsMissingAndResolved() {
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

    String value = resolved.get().getCorrelationId();
    assertNotNull(value);
    assertFalse(value.isEmpty());
  }

  @Test
  void correlationId_nullContext_sdkGeneratesUuidAndStamps() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<String> mdcCorrelation = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          mdcCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
          return null;
        });

    assertNotNull(mdcCorrelation.get(), "SDK always stamps MDC even with null context");
    SpanData span = otel.getSpans().get(0);
    assertNotNull(
        span.getAttributes().get(AttributeKey.stringKey(
            MultiCloudJLogger.CORRELATION_ID_METADATA_KEY)));
  }

  // --- Fixed SDK MDC fields ------------------------------------------------

  @Test
  void mdcPopulated_during_clearedAfter() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<String> capturedTraceId = new AtomicReference<>();
    AtomicReference<String> capturedSpanId = new AtomicReference<>();
    AtomicReference<String> capturedSdkService = new AtomicReference<>();
    AtomicReference<String> capturedSdkProvider = new AtomicReference<>();
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        OperationContext.builder().build(),
        ctx -> {
          capturedTraceId.set(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
          capturedSpanId.set(MDC.get(MultiCloudJLogger.MDC_SPAN_ID));
          capturedSdkService.set(MDC.get(MultiCloudJLogger.MDC_SDK_SERVICE));
          capturedSdkProvider.set(MDC.get(MultiCloudJLogger.MDC_SDK_PROVIDER));
          capturedCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
          return null;
        });

    assertNotNull(capturedTraceId.get());
    assertNotNull(capturedSpanId.get());
    assertEquals("blob", capturedSdkService.get());
    assertEquals("aws", capturedSdkProvider.get());
    assertNotNull(capturedCorrelation.get());

    assertNull(MDC.get(MultiCloudJLogger.MDC_TRACE_ID));
    assertNull(MDC.get(MultiCloudJLogger.MDC_SPAN_ID));
    assertNull(MDC.get(MultiCloudJLogger.MDC_SDK_SERVICE));
    assertNull(MDC.get(MultiCloudJLogger.MDC_SDK_PROVIDER));
    assertNull(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
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
                OperationContext.builder().build(),
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
            OperationContext.builder().build(),
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

    ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
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
  void traceAsyncOperation_disabled_noSpanButCorrelationInMdc()
      throws ExecutionException, InterruptedException {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "aws");
    AtomicReference<String> capturedCorrelation = new AtomicReference<>();

    CompletableFuture<String> future =
        logger.traceAsyncOperation(
            "blob.test.async",
            null,
            OperationContext.builder().build(),
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

  // --- tenantId ------------------------------------------------------------

  @Test
  void tenantId_provided_setOnSpanAttributeAndMdc() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    OperationContext input =
        OperationContext.builder()
            .correlationId("req-1")
            .tenantId("tenant-42")
            .build();
    AtomicReference<String> capturedTenant = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        input,
        ctx -> {
          capturedTenant.set(MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
          return null;
        });

    assertEquals("tenant-42", capturedTenant.get(), "tenant_id should be in MDC during operation");
    assertEquals(
        "tenant-42",
        otel.getSpans().get(0).getAttributes().get(AttributeKey.stringKey("tenant_id")),
        "tenant_id should be a span attribute");
    assertNull(MDC.get(MultiCloudJLogger.MDC_TENANT_ID), "tenant_id MDC must be cleared after");
  }

  @Test
  void tenantId_null_notSetOnSpanAttribute_andNotInMdc() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<String> capturedTenant = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          capturedTenant.set(MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
          return null;
        });

    assertNull(capturedTenant.get(), "tenant_id MDC should not be set when tenant is missing");
    assertNull(
        otel.getSpans().get(0).getAttributes().get(AttributeKey.stringKey("tenant_id")),
        "tenant_id span attribute should not be set when tenant is missing");
  }

  @Test
  void tenantId_disabledPolicy_stillInMdc() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "aws");
    OperationContext input = OperationContext.builder().tenantId("tenant-7").build();
    AtomicReference<String> capturedTenant = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        input,
        ctx -> {
          capturedTenant.set(MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
          return null;
        });

    assertEquals("tenant-7", capturedTenant.get());
    assertTrue(otel.getSpans().isEmpty(), "no spans under DISABLED");
    assertNull(MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
  }

  @Test
  void tenantId_clearedAfterException() {
    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    OperationContext input = OperationContext.builder().tenantId("tenant-x").build();

    assertThrows(
        IllegalStateException.class,
        () ->
            logger.traceOperation(
                "blob.test",
                null,
                input,
                ctx -> {
                  throw new IllegalStateException("boom");
                }));

    assertNull(MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
  }

  @Test
  void tenantId_neverAutoGenerated() {
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

    assertNull(resolved.get().getTenantId(), "tenant_id must NOT be auto-generated");
  }

  // --- prior MDC preservation ---------------------------------------------

  @Test
  void priorCorrelationMdc_isRestoredAfterTrace_underTracedPolicy() {
    MDC.put(MultiCloudJLogger.MDC_CORRELATION_ID, "outer-correlation");
    MDC.put(MultiCloudJLogger.MDC_TENANT_ID, "outer-tenant");
    MDC.put(MultiCloudJLogger.MDC_SDK_SERVICE, "outer-svc");
    MDC.put(MultiCloudJLogger.MDC_SDK_PROVIDER, "outer-provider");

    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");
    AtomicReference<String> insideCorrelation = new AtomicReference<>();
    AtomicReference<String> insideTenant = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        Map.of("bucket", "b1"),
        OperationContext.builder()
            .correlationId("inner-correlation")
            .tenantId("inner-tenant")
            .build(),
        ctx -> {
          insideCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
          insideTenant.set(MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
          return null;
        });

    assertEquals(
        "inner-correlation",
        insideCorrelation.get(),
        "SDK's correlation value must be visible inside the lambda");
    assertEquals(
        "inner-tenant", insideTenant.get(), "SDK's tenant_id must be visible inside the lambda");

    assertEquals(
        "outer-correlation",
        MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID),
        "prior correlation value must be restored after the trace");
    assertEquals(
        "outer-tenant",
        MDC.get(MultiCloudJLogger.MDC_TENANT_ID),
        "prior tenant_id must be restored");
    assertEquals(
        "outer-svc",
        MDC.get(MultiCloudJLogger.MDC_SDK_SERVICE),
        "prior sdk_service must be restored");
    assertEquals(
        "outer-provider",
        MDC.get(MultiCloudJLogger.MDC_SDK_PROVIDER),
        "prior sdk_provider must be restored");
  }

  @Test
  void priorCorrelationMdc_isRestoredAfterTrace_underDisabledPolicy() {
    MDC.put(MultiCloudJLogger.MDC_CORRELATION_ID, "outer-correlation");
    MDC.put(MultiCloudJLogger.MDC_TENANT_ID, "outer-tenant");

    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.DISABLED, "blob", "aws");
    AtomicReference<String> insideCorrelation = new AtomicReference<>();

    logger.traceOperation(
        "blob.test",
        null,
        OperationContext.builder()
            .correlationId("inner-correlation")
            .build(),
        ctx -> {
          insideCorrelation.set(MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID));
          return null;
        });

    assertEquals("inner-correlation", insideCorrelation.get());
    assertEquals(
        "outer-correlation",
        MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID),
        "DISABLED-policy quiet path must also restore prior MDC values");
    assertEquals("outer-tenant", MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
  }

  @Test
  void priorCorrelationMdc_isRestoredAfterTrace_evenOnException() {
    MDC.put(MultiCloudJLogger.MDC_CORRELATION_ID, "outer-correlation");

    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    assertThrows(
        IllegalStateException.class,
        () ->
            logger.traceOperation(
                "blob.test",
                null,
                OperationContext.builder()
                    .correlationId("inner-correlation")
                    .build(),
                ctx -> {
                  throw new IllegalStateException("boom");
                }));

    assertEquals(
        "outer-correlation",
        MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID),
        "prior MDC values must be restored even when the operation throws");
  }

  @Test
  void nonSdkMdcKeys_arePassedThroughUnchanged() {
    MDC.put("custom_key", "custom_value");
    MDC.put("request_id", "req-42");

    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    logger.traceOperation(
        "blob.test",
        null,
        null,
        ctx -> {
          assertEquals(
              "custom_value",
              MDC.get("custom_key"),
              "non-SDK keys must remain visible inside the lambda");
          MDC.put("set_inside_lambda", "yes");
          return null;
        });

    assertEquals(
        "custom_value", MDC.get("custom_key"), "non-SDK keys must remain after the trace returns");
    assertEquals("req-42", MDC.get("request_id"));
    assertEquals(
        "yes",
        MDC.get("set_inside_lambda"),
        "non-SDK keys set inside the lambda must persist past the trace");
  }

  @Test
  void priorCorrelationMdc_isRestoredAfterAsyncTrace_successPath() {
    MDC.put(MultiCloudJLogger.MDC_CORRELATION_ID, "outer-correlation");
    MDC.put(MultiCloudJLogger.MDC_TENANT_ID, "outer-tenant");

    MultiCloudJLogger logger = new MultiCloudJLogger(TracingPolicy.CHILD_AND_ROOT, "blob", "aws");

    CompletableFuture<String> future =
        logger.traceAsyncOperation(
            "blob.test.async",
            Map.of("bucket", "b1"),
            OperationContext.builder()
                .correlationId("inner-correlation")
                .tenantId("inner-tenant")
                .build(),
            ctx -> CompletableFuture.completedFuture("ok"));

    assertEquals("ok", future.join());
    assertEquals(
        "outer-correlation",
        MDC.get(MultiCloudJLogger.MDC_CORRELATION_ID),
        "prior MDC must be restored after async trace");
    assertEquals("outer-tenant", MDC.get(MultiCloudJLogger.MDC_TENANT_ID));
  }
}
