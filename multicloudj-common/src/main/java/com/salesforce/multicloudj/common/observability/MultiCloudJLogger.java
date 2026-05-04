package com.salesforce.multicloudj.common.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Centralized observability utility for SDK service implementations.
 *
 * <p>Wraps service operations with OpenTelemetry span lifecycle, MDC management, and SLF4J
 * logging subject to the configured {@link TracingPolicy}. Constructed once per service client
 * (e.g. once per {@code AbstractBlobStore}). Provider implementations never interact with this
 * class directly; the abstract layer of each service does the wrapping.
 *
 * <p>Three policies are supported (see {@link TracingPolicy}). Under {@code DISABLED} or {@code
 * JOIN_ONLY}-with-no-parent, no span is created but {@code correlation_id}, {@code sdk_service}
 * and {@code sdk_provider} are still populated in MDC so application logs can be correlated.
 */
public class MultiCloudJLogger {

  private static final Logger log = LoggerFactory.getLogger(MultiCloudJLogger.class);
  private static final String TRACER_NAME = "com.salesforce.multicloudj";

  static final String MDC_TRACE_ID = "trace_id";
  static final String MDC_SPAN_ID = "span_id";
  static final String MDC_CORRELATION_ID = "correlation_id";
  static final String MDC_SDK_SERVICE = "sdk_service";
  static final String MDC_SDK_PROVIDER = "sdk_provider";
  static final String MDC_TENANT_ID = "tenant_id";

  static final String ATTR_BUCKET = "bucket";
  static final String ATTR_CORRELATION_ID = "correlation_id";
  static final String ATTR_SDK_SERVICE = "sdk_service";
  static final String ATTR_SDK_PROVIDER = "sdk_provider";
  static final String ATTR_TENANT_ID = "tenant_id";

  private final TracingPolicy policy;
  private final String serviceName;
  private final String providerId;

  /**
   * Creates a logger for a service client.
   *
   * @param policy the per-client tracing policy; if {@code null}, the global default is used
   * @param serviceName the SDK service name (e.g. {@code "blob"})
   * @param providerId the provider identifier (e.g. {@code "aws"}, {@code "gcp"})
   */
  public MultiCloudJLogger(TracingPolicy policy, String serviceName, String providerId) {
    this.policy = policy != null ? policy : MultiCloudJObservability.getDefaultTracingPolicy();
    this.serviceName = serviceName;
    this.providerId = providerId;
  }

  /**
   * Wraps a value-returning operation with the configured tracing/logging policy.
   *
   * <p>The supplied {@code operation} receives the resolved {@link OperationContext} (correlation
   * ID guaranteed non-null) so the result can echo it back to the caller via the response.
   *
   * @param operationName the span name, e.g. {@code blob.upload}
   * @param attributes optional span attributes such as {@code bucket}; may be {@code null}
   * @param operationContext the per-call observability context; may be {@code null}
   * @param operation the work to perform
   * @param <T> the result type
   * @return whatever {@code operation} returns
   */
  public <T> T traceOperation(
      String operationName,
      Map<String, String> attributes,
      OperationContext operationContext,
      Function<OperationContext, T> operation) {
    OperationContext effectiveContext = resolveContext(operationContext);
    String bucket = bucketFrom(attributes);

    // Policy decision matrix (see TracingPolicy):
    //   DISABLED         | (any)        -> no span, quiet MDC only
    //   JOIN_ONLY        | no parent    -> no span, quiet MDC only
    //   JOIN_ONLY        | has parent   -> child span, full MDC
    //   CHILD_AND_ROOT   | no parent    -> root span,  full MDC
    //   CHILD_AND_ROOT   | has parent   -> child span, full MDC
    if (policy == TracingPolicy.DISABLED) {
      return executeQuietly(effectiveContext, bucket, operationName, operation);
    }
    boolean hasParent = Span.current().getSpanContext().isValid();
    if (!hasParent && policy == TracingPolicy.JOIN_ONLY) {
      return executeQuietly(effectiveContext, bucket, operationName, operation);
    }

    Span span = startSpan(operationName, attributes, effectiveContext, hasParent);
    long startTime = System.nanoTime();

    try (Scope ignored = span.makeCurrent()) {
      setMdc(span, effectiveContext);
      try {
        log.debug("{} started [bucket={}]", operationName, bucket);
        T result = operation.apply(effectiveContext);
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        log.debug("{} completed [bucket={}, duration={}ms]", operationName, bucket, durationMs);
        return result;
      } catch (RuntimeException e) {
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        span.recordException(e);
        span.setStatus(StatusCode.ERROR, e.getMessage());
        log.error("{} failed [bucket={}, duration={}ms]", operationName, bucket, durationMs, e);
        throw e;
      } finally {
        clearMdc();
      }
    } finally {
      span.end();
    }
  }

  /**
   * Wraps a void operation. See {@link #traceOperation}.
   *
   * @param operationName the span name
   * @param attributes optional span attributes; may be {@code null}
   * @param operationContext the per-call observability context; may be {@code null}
   * @param operation the work to perform
   */
  public void traceVoidOperation(
      String operationName,
      Map<String, String> attributes,
      OperationContext operationContext,
      Consumer<OperationContext> operation) {
    traceOperation(
        operationName,
        attributes,
        operationContext,
        ctx -> {
          operation.accept(ctx);
          return null;
        });
  }

  /**
   * Wraps an asynchronous operation that returns a {@link CompletableFuture}.
   *
   * <p>The OpenTelemetry context and MDC are populated for the synchronous portion of {@code
   * operation} (i.e. while it is being invoked to obtain the future). The span is ended (and any
   * exception recorded) when the returned future completes.
   *
   * @param operationName the span name
   * @param attributes optional span attributes; may be {@code null}
   * @param operationContext the per-call observability context; may be {@code null}
   * @param operation supplier that produces the future; receives the resolved context
   * @param <T> the eventual result type
   * @return a future that completes when the underlying future completes
   */
  public <T> CompletableFuture<T> traceAsyncOperation(
      String operationName,
      Map<String, String> attributes,
      OperationContext operationContext,
      Function<OperationContext, CompletableFuture<T>> operation) {
    OperationContext effectiveContext = resolveContext(operationContext);
    String bucket = bucketFrom(attributes);

    if (policy == TracingPolicy.DISABLED) {
      return executeAsyncQuietly(effectiveContext, bucket, operationName, operation);
    }
    boolean hasParent = Span.current().getSpanContext().isValid();
    if (!hasParent && policy == TracingPolicy.JOIN_ONLY) {
      return executeAsyncQuietly(effectiveContext, bucket, operationName, operation);
    }

    Span span = startSpan(operationName, attributes, effectiveContext, hasParent);
    long startTime = System.nanoTime();

    CompletableFuture<T> future;
    try (Scope ignored = span.makeCurrent()) {
      setMdc(span, effectiveContext);
      try {
        log.debug("{} started [bucket={}]", operationName, bucket);
        future = operation.apply(effectiveContext);
      } catch (RuntimeException e) {
        long durationMs = (System.nanoTime() - startTime) / 1_000_000;
        span.recordException(e);
        span.setStatus(StatusCode.ERROR, e.getMessage());
        log.error("{} failed [bucket={}, duration={}ms]", operationName, bucket, durationMs, e);
        span.end();
        throw e;
      } finally {
        clearMdc();
      }
    }

    if (future == null) {
      span.end();
      return null;
    }

    return future.whenComplete(
        (result, throwable) -> {
          long durationMs = (System.nanoTime() - startTime) / 1_000_000;
          try (Scope ignored = span.makeCurrent()) {
            setMdc(span, effectiveContext);
            try {
              if (throwable != null) {
                Throwable cause = unwrap(throwable);
                span.recordException(cause);
                span.setStatus(StatusCode.ERROR, cause.getMessage());
                log.error(
                    "{} failed [bucket={}, duration={}ms]",
                    operationName, bucket, durationMs, cause);
              } else {
                log.debug(
                    "{} completed [bucket={}, duration={}ms]",
                    operationName, bucket, durationMs);
              }
            } finally {
              clearMdc();
            }
          } finally {
            span.end();
          }
        });
  }

  private Span startSpan(
      String operationName,
      Map<String, String> attributes,
      OperationContext effectiveContext,
      boolean hasParent) {
    Tracer tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
    SpanBuilder spanBuilder = tracer.spanBuilder(operationName);
    if (!hasParent) {
      spanBuilder.setNoParent();
    }
    if (attributes != null) {
      attributes.forEach(spanBuilder::setAttribute);
    }
    spanBuilder.setAttribute(ATTR_CORRELATION_ID, effectiveContext.getCorrelationId());
    spanBuilder.setAttribute(ATTR_SDK_SERVICE, serviceName);
    spanBuilder.setAttribute(ATTR_SDK_PROVIDER, providerId);
    if (effectiveContext.getTenantId() != null) {
      spanBuilder.setAttribute(ATTR_TENANT_ID, effectiveContext.getTenantId());
    }
    return spanBuilder.startSpan();
  }

  private <T> T executeQuietly(
      OperationContext effectiveContext,
      String bucket,
      String operationName,
      Function<OperationContext, T> operation) {
    long startTime = System.nanoTime();
    setQuietMdc(effectiveContext);
    try {
      log.debug("{} started [bucket={}]", operationName, bucket);
      T result = operation.apply(effectiveContext);
      long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      log.debug("{} completed [bucket={}, duration={}ms]", operationName, bucket, durationMs);
      return result;
    } catch (RuntimeException e) {
      long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      log.error("{} failed [bucket={}, duration={}ms]", operationName, bucket, durationMs, e);
      throw e;
    } finally {
      clearQuietMdc();
    }
  }

  private <T> CompletableFuture<T> executeAsyncQuietly(
      OperationContext effectiveContext,
      String bucket,
      String operationName,
      Function<OperationContext, CompletableFuture<T>> operation) {
    long startTime = System.nanoTime();
    setQuietMdc(effectiveContext);
    CompletableFuture<T> future;
    try {
      log.debug("{} started [bucket={}]", operationName, bucket);
      future = operation.apply(effectiveContext);
    } catch (RuntimeException e) {
      long durationMs = (System.nanoTime() - startTime) / 1_000_000;
      log.error("{} failed [bucket={}, duration={}ms]", operationName, bucket, durationMs, e);
      throw e;
    } finally {
      clearQuietMdc();
    }
    if (future == null) {
      return null;
    }
    return future.whenComplete(
        (result, throwable) -> {
          long durationMs = (System.nanoTime() - startTime) / 1_000_000;
          setQuietMdc(effectiveContext);
          try {
            if (throwable != null) {
              Throwable cause = unwrap(throwable);
              log.error(
                  "{} failed [bucket={}, duration={}ms]",
                  operationName, bucket, durationMs, cause);
            } else {
              log.debug(
                  "{} completed [bucket={}, duration={}ms]",
                  operationName, bucket, durationMs);
            }
          } finally {
            clearQuietMdc();
          }
        });
  }

  private OperationContext resolveContext(OperationContext context) {
    if (context == null) {
      return OperationContext.builder().correlationId(generateCorrelationId()).build();
    }
    String existing = context.getCorrelationId();
    if (existing == null || existing.isEmpty()) {
      return context.toBuilder().correlationId(generateCorrelationId()).build();
    }
    return context;
  }

  private static String generateCorrelationId() {
    return UUID.randomUUID().toString();
  }

  private static String bucketFrom(Map<String, String> attributes) {
    return attributes != null ? attributes.get(ATTR_BUCKET) : null;
  }

  private static Throwable unwrap(Throwable t) {
    return (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;
  }

  private void setMdc(Span span, OperationContext effectiveContext) {
    MDC.put(MDC_TRACE_ID, span.getSpanContext().getTraceId());
    MDC.put(MDC_SPAN_ID, span.getSpanContext().getSpanId());
    MDC.put(MDC_CORRELATION_ID, effectiveContext.getCorrelationId());
    MDC.put(MDC_SDK_SERVICE, serviceName);
    MDC.put(MDC_SDK_PROVIDER, providerId);
    if (effectiveContext.getTenantId() != null) {
      MDC.put(MDC_TENANT_ID, effectiveContext.getTenantId());
    }
  }

  private void clearMdc() {
    MDC.remove(MDC_TRACE_ID);
    MDC.remove(MDC_SPAN_ID);
    MDC.remove(MDC_CORRELATION_ID);
    MDC.remove(MDC_SDK_SERVICE);
    MDC.remove(MDC_SDK_PROVIDER);
    MDC.remove(MDC_TENANT_ID);
  }

  private void setQuietMdc(OperationContext effectiveContext) {
    MDC.put(MDC_CORRELATION_ID, effectiveContext.getCorrelationId());
    MDC.put(MDC_SDK_SERVICE, serviceName);
    MDC.put(MDC_SDK_PROVIDER, providerId);
    if (effectiveContext.getTenantId() != null) {
      MDC.put(MDC_TENANT_ID, effectiveContext.getTenantId());
    }
  }

  private void clearQuietMdc() {
    MDC.remove(MDC_CORRELATION_ID);
    MDC.remove(MDC_SDK_SERVICE);
    MDC.remove(MDC_SDK_PROVIDER);
    MDC.remove(MDC_TENANT_ID);
  }
}
