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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMH benchmarks for IAM control-plane operations via {@link IamClient}.
 *
 * <p>IAM is latency-primary: the policy backend throttles hard, so throughput mostly reflects the
 * provider's rate limiter. The {@code SampleTime} percentiles are the signal; {@code Throughput}
 * is kept only for trend comparison.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// No @Threads: benchmarks read-modify-write one shared IAM policy; concurrent invocations would
// race and corrupt each other's bindings.
public abstract class AbstractIamBenchmarkTest {

  private static final Logger logger = LoggerFactory.getLogger(AbstractIamBenchmarkTest.class);

  // Minted during setup, drained in cleanup() so a run never leaks real principals (a deleted
  // identity may still count against an account quota during a retention window).
  private final Set<String> createdIdentities = ConcurrentHashMap.newKeySet();

  private String lifecycleIdentityName;
  private String lifecyclePolicyMember;

  // cleanup() runs from both @TearDown and a shutdown hook (JMH skips @TearDown on an aborted
  // trial); this guard makes whichever loses the race a no-op.
  private final AtomicBoolean cleaned = new AtomicBoolean(false);
  private Thread cleanupHook;

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

    // Kept short: some providers cap identity ids (~30 chars); callers append a numeric suffix.
    default String getLifecycleIdentityPrefix() {
      return "iam-bench-lc-";
    }

    /** Identity form the attach/remove calls expect; providers needing a typed member override. */
    default String toPolicyMember(String identityName, String identityId) {
      return identityName;
    }

    /** Pre-seeded read identity ({@link #getIdentityName()}) as the policy calls expect it. */
    default String getPolicyMemberName() {
      return toPolicyMember(getIdentityName(), getIdentityName());
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
    // Arm before any RPC that can throw: identities are registered as minted, so the hook drains
    // them even if setup fails partway.
    cleanupHook = new Thread(this::cleanup, "iam-benchmark-cleanup");
    Runtime.getRuntime().addShutdownHook(cleanupHook);
    try {
      harness = createHarness();
      iamClient = harness.createIamClient();

      // Seed the inline policy the read benchmarks consume, via the member form: seeding with the
      // bare name is rejected on providers needing a typed member, leaving the reads measuring an
      // empty lookup. Tolerate an already-attached policy (aborted trial) but log it — some
      // providers fail the seed silently.
      try {
        iamClient.attachInlinePolicy(
            AttachInlinePolicyRequest.builder()
                .policyDocument(buildPolicyDocument(harness.getPolicyName()))
                .tenantId(harness.getTenantId())
                .region(harness.getRegion())
                .identityName(harness.getPolicyMemberName())
                .build());
      } catch (Exception e) {
        logger.warn(
            "Seed attachInlinePolicy failed (may already be attached): {}", e.getMessage());
      }

      // Create the lifecycle identity once here, outside any timed region; nanoTime suffix so
      // re-runs and forks never collide on names.
      String identityName =
          harness.getLifecycleIdentityPrefix() + (System.nanoTime() & 0xFFFFFFFFL);
      // Track before creating: a create RPC can commit server-side and still surface an error to
      // the client, so the name must be drainable even if createIdentity throws. Deleting a
      // never-created name is swallowed downstream.
      createdIdentities.add(identityName);
      String identityId =
          iamClient.createIdentity(
              identityName,
              "IAM benchmark lifecycle identity",
              harness.getTenantId(),
              harness.getRegion(),
              Optional.empty(),
              Optional.empty());
      lifecycleIdentityName = identityName;
      lifecyclePolicyMember = harness.toPolicyMember(identityName, identityId);

      // Prime propagation here, not in the benchmark, so the timed region never pays for it.
      attachWithPropagationRetry(harness.getPolicyName(), lifecyclePolicyMember);
      iamClient.removePolicy(
          lifecyclePolicyMember,
          harness.getPolicyName(),
          harness.getTenantId(),
          harness.getRegion());
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() {
    cleanup();
    if (cleanupHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(cleanupHook);
      } catch (IllegalStateException e) {
        // Shutdown already in progress — the hook is running/ran; nothing to remove.
      }
    }
  }

  /**
   * Removes the seed policy, drains minted identities, closes client/harness. Runs once (guarded
   * by {@link #cleaned}); swallows per-step failures so one bad delete can't strand the rest.
   */
  private void cleanup() {
    if (!cleaned.compareAndSet(false, true)) {
      return;
    }
    if (iamClient != null) {
      if (harness != null) {
        try {
          iamClient.removePolicy(
              harness.getPolicyMemberName(),
              harness.getPolicyName(),
              harness.getTenantId(),
              harness.getRegion());
        } catch (Exception e) {
          logger.warn("Failed to remove seed policy in cleanup: {}", e.getMessage());
        }
      }

      for (String identityName : createdIdentities) {
        try {
          iamClient.deleteIdentity(identityName, harness.getTenantId(), harness.getRegion());
        } catch (Exception e) {
          logger.warn("Failed to delete benchmark identity {}: {}", identityName, e.getMessage());
        }
      }
      createdIdentities.clear();

      try {
        iamClient.close();
      } catch (Exception e) {
        logger.warn("Failed to close IAM client in cleanup: {}", e.getMessage());
      }
    }

    if (harness != null) {
      try {
        harness.close();
      } catch (Exception e) {
        logger.warn("Failed to close harness in cleanup: {}", e.getMessage());
      }
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
                  .identityName(harness.getPolicyMemberName())
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
                  .identityName(harness.getPolicyMemberName())
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

  /**
   * Attach then remove an inline policy on an already-propagated identity — times only the two
   * mutating RPCs; create and delete happen in setup/cleanup. Reuses {@code getPolicyName()} for
   * both calls: some providers match removePolicy on the role translated from the actions, not the
   * document name, so attach and remove must name the same role.
   */
  @Benchmark
  public void benchmarkAttachRemovePolicy(Blackhole bh) {
    String policyName = harness.getPolicyName();
    iamClient.attachInlinePolicy(
        AttachInlinePolicyRequest.builder()
            .policyDocument(buildPolicyDocument(policyName))
            .tenantId(harness.getTenantId())
            .region(harness.getRegion())
            .identityName(lifecyclePolicyMember)
            .build());
    iamClient.removePolicy(
        lifecyclePolicyMember, policyName, harness.getTenantId(), harness.getRegion());
    bh.consume(lifecycleIdentityName);
  }

  // Bounded retry to absorb create->attach propagation lag on eventually-consistent providers.
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
