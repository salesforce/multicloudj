package com.salesforce.multicloudj.iam.client;

import com.salesforce.multicloudj.iam.model.Action;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.Effect;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmarks for IAM control-plane operations via {@link IamClient}.
 *
 * <p>IAM is latency-primary: the SetIamPolicy/GetIamPolicy backend throttles hard, so throughput
 * mostly reflects the provider's rate limiter. The {@code SampleTime} percentiles are the signal;
 * {@code Throughput} is kept only for trend comparison.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// No @Threads: benchmarks read-modify-write a single shared IAM policy (project or role), so
// concurrent invocations would race on GetIamPolicy/SetIamPolicy and corrupt each other's bindings.
public abstract class AbstractIamBenchmarkTest {

  private final AtomicInteger nextLifecycleId = new AtomicInteger(0);

  // Harness interface
  public interface Harness extends AutoCloseable {
    IamClient createIamClient();

    String getIdentityName();

    String getTenantId();

    String getRegion();

    String getPolicyName();

    String getRoleName();

    List<String> getPolicyActions();

    default String getPolicyResource() {
      return null;
    }

    default String getPolicyVersion() {
      return "";
    }

    default String getLifecycleIdentityPrefix() {
      return "iam-benchmark-lifecycle-";
    }

    /**
     * Converts a raw identity name/id into the form the provider's attach/remove policy calls
     * expect as {@code identityName} (e.g. GCP needs a "serviceAccount:email" member string,
     * not the bare account id used to create the identity).
     */
    default String toPolicyMember(String identityName, String identityId) {
      return identityName;
    }
  }

  protected Harness harness;
  protected IamClient iamClient;

  protected abstract Harness createHarness();

  protected abstract String getProviderId();

  /**
   * Reads a required config value from OS environment first, then from -D system properties.
   * Fails fast with a clear error if neither is set.
   */
  protected static String requireEnv(String name) {
    String value = System.getenv(name);
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(name);
    }
    if (StringUtils.isBlank(value)) {
      throw new IllegalStateException("Required environment variable not set: " + name);
    }
    return value;
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws Exception {
    try {
      harness = createHarness();
      iamClient = harness.createIamClient();

      // Seed the inline policy that benchmarkGetAttachedPolicies / benchmarkGetInlinePolicyDetails
      // read. Tolerate an already-attached policy (e.g. left over from a previously aborted trial):
      // the read benchmarks only need it to exist, and teardown removes it. A hard failure here
      // would wedge every benchmark method in the class.
      try {
        iamClient.attachInlinePolicy(
            AttachInlinePolicyRequest.builder()
                .policyDocument(buildPolicyDocument(harness.getPolicyName()))
                .tenantId(harness.getTenantId())
                .region(harness.getRegion())
                .identityName(harness.getIdentityName())
                .build());
      } catch (Exception e) {
        // Best-effort seed; the policy may already be attached. Read benchmarks tolerate this.
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() throws Exception {
    try {
      if (iamClient != null) {
        try {
          iamClient.removePolicy(
              harness.getIdentityName(),
              harness.getPolicyName(),
              harness.getTenantId(),
              harness.getRegion());
        } catch (Exception e) {
          // Continue cleanup on error
        }
        iamClient.close();
      }

      if (harness != null) {
        harness.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error closing harness", e);
    }
  }

  private PolicyDocument buildPolicyDocument(String policyName) {
    Statement.StatementBuilder statementBuilder = Statement.builder().effect(Effect.ALLOW);
    if (harness.getPolicyResource() != null) {
      statementBuilder.resource(harness.getPolicyResource());
    }
    for (String action : harness.getPolicyActions()) {
      statementBuilder.action(Action.of(action));
    }
    return PolicyDocument.builder()
        .name(policyName)
        .version(harness.getPolicyVersion())
        .statement(statementBuilder.build())
        .build();
  }

  /** Get Identity - read steady-state, the most common op. */
  @Benchmark
  public void benchmarkGetIdentity(Blackhole bh) {
    try {
      String identity =
          iamClient.getIdentity(
              harness.getIdentityName(), harness.getTenantId(), harness.getRegion());
      bh.consume(identity);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get identity failed", e);
    }
  }

  /** Get Attached Policies - list, possibly paginated. */
  @Benchmark
  public void benchmarkGetAttachedPolicies(Blackhole bh) {
    try {
      List<String> attachedPolicies =
          iamClient.getAttachedPolicies(
              GetAttachedPoliciesRequest.builder()
                  .roleName(harness.getRoleName())
                  .identityName(harness.getIdentityName())
                  .tenantId(harness.getTenantId())
                  .region(harness.getRegion())
                  .build());
      bh.consume(attachedPolicies);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get attached policies failed", e);
    }
  }

  /** Get Inline Policy Details - fetch + parse. */
  @Benchmark
  public void benchmarkGetInlinePolicyDetails(Blackhole bh) {
    try {
      String policyDetails =
          iamClient.getInlinePolicyDetails(
              GetInlinePolicyDetailsRequest.builder()
                  .identityName(harness.getIdentityName())
                  .policyName(harness.getPolicyName())
                  .roleName(harness.getRoleName())
                  .tenantId(harness.getTenantId())
                  .region(harness.getRegion())
                  .build());
      bh.consume(policyDetails);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get inline policy details failed", e);
    }
  }

  /** Create/Attach/Remove/Delete identity lifecycle; bundled so mutating ops self-clean. */
  @Benchmark
  public void benchmarkCreateAttachDeleteIdentity(Blackhole bh) {
    String identityName = harness.getLifecycleIdentityPrefix() + nextLifecycleId.incrementAndGet();
    // Reuse harness.getPolicyName() rather than a per-invocation name: GCP's removePolicy matches
    // against the role translated from policy actions (not the document name), so the name must be
    // identical between attach and remove. Uniqueness comes from identityName instead.
    String lifecyclePolicyName = harness.getPolicyName();
    boolean identityCreated = false;

    try {
      String identityId =
          iamClient.createIdentity(
              identityName,
              "IAM benchmark lifecycle identity",
              harness.getTenantId(),
              harness.getRegion(),
              Optional.empty(),
              Optional.empty());
      identityCreated = true;
      bh.consume(identityId);

      String policyMember = harness.toPolicyMember(identityName, identityId);

      // Some providers create identities eventually-consistently: the attach can race propagation
      // and fail with "does not exist". Retry briefly; read-after-write providers pass first try.
      attachWithPropagationRetry(lifecyclePolicyName, policyMember);

      iamClient.removePolicy(
          policyMember, lifecyclePolicyName, harness.getTenantId(), harness.getRegion());
    } finally {
      if (identityCreated) {
        try {
          iamClient.deleteIdentity(identityName, harness.getTenantId(), harness.getRegion());
        } catch (Exception e) {
          // Guard cleanup so a failure mid-sequence doesn't wedge the trial
        }
      }
    }
  }

  // Bounded retry to absorb create->attach eventual-consistency lag (identity not yet propagated).
  private static final int ATTACH_MAX_ATTEMPTS = 5;
  private static final long ATTACH_RETRY_BASE_MILLIS = 500L;

  private void attachWithPropagationRetry(String policyName, String policyMember) {
    RuntimeException last = null;
    for (int attempt = 1; attempt <= ATTACH_MAX_ATTEMPTS; attempt++) {
      try {
        iamClient.attachInlinePolicy(
            AttachInlinePolicyRequest.builder()
                .policyDocument(buildPolicyDocument(policyName))
                .tenantId(harness.getTenantId())
                .region(harness.getRegion())
                .identityName(policyMember)
                .build());
        return;
      } catch (RuntimeException e) {
        last = e;
        if (!isIdentityNotPropagated(e) || attempt == ATTACH_MAX_ATTEMPTS) {
          throw e;
        }
        try {
          Thread.sleep(ATTACH_RETRY_BASE_MILLIS * attempt);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    throw last;
  }

  private static boolean isIdentityNotPropagated(Throwable t) {
    for (Throwable cur = t; cur != null; cur = cur.getCause()) {
      String msg = cur.getMessage();
      if (msg != null && msg.toLowerCase().contains("does not exist")) {
        return true;
      }
    }
    return false;
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("IAM_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    Options opt =
        new OptionsBuilder()
            .include(".*" + this.getClass().getName() + ".*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-iam-results-" + getProviderId() + ".json")
            .jvmArgsAppend(forwardedArgs.toArray(new String[0]))
            .build();

    new Runner(opt).run();
  }
}
