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

  private static final Logger logger = LoggerFactory.getLogger(AbstractIamBenchmarkTest.class);

  // Identities minted during setup; drained in @TearDown so a run never leaks real cloud
  // principals (e.g. GCP soft-deleted service accounts count against the 100/project quota
  // for 30 days). ConcurrentHashMap.newKeySet() mirrors AbstractDocstoreBenchmarkTest's pattern.
  private final Set<String> createdIdentities = ConcurrentHashMap.newKeySet();

  // Lifecycle identity created once in @Setup and reused by benchmarkAttachRemovePolicy so the
  // create RPC and its eventual-consistency wait stay OUT of the timed region.
  private String lifecycleIdentityName;
  private String lifecyclePolicyMember;

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

    // Kept short: GCP service-account ids cap at 30 chars, and callers append a numeric suffix.
    default String getLifecycleIdentityPrefix() {
      return "iam-bench-lc-";
    }

    /**
     * Converts a raw identity name/id into the form the provider's attach/remove policy calls
     * expect as {@code identityName} (e.g. GCP needs a "serviceAccount:email" member string,
     * not the bare account id used to create the identity).
     */
    default String toPolicyMember(String identityName, String identityId) {
      return identityName;
    }

    /**
     * The member string for the pre-seeded read identity ({@link #getIdentityName()}) as
     * attach/remove/get-policy expect it. GCP needs "serviceAccount:email" here, not the bare
     * email that {@code getIdentity} consumes; AWS is role-scoped and unaffected.
     */
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
    try {
      harness = createHarness();
      iamClient = harness.createIamClient();

      // Seed the inline policy that benchmarkGetAttachedPolicies / benchmarkGetInlinePolicyDetails
      // read. Use the member form (GCP needs "serviceAccount:email"): with the bare identity name
      // the GCP SetIamPolicy is rejected, and both read benchmarks would then measure a
      // silently-empty lookup instead of a populated policy. Tolerate an already-attached policy
      // (e.g. left over from an aborted trial) but log it: on AWS a missing seed throws loudly, on
      // GCP it is silent, so an unlogged swallow hides the failure on the provider where it bites.
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

      // Create the lifecycle identity ONCE here, absorbing create + eventual-consistency lag
      // outside any timed region, so benchmarkAttachRemovePolicy times only attach/remove against
      // an already-propagated principal. Suffix with nanoTime so re-runs (and forks, which restart
      // JMH counters at 0) never collide on names.
      String identityName =
          harness.getLifecycleIdentityPrefix() + (System.nanoTime() & 0xFFFFFFFFL);
      String identityId =
          iamClient.createIdentity(
              identityName,
              "IAM benchmark lifecycle identity",
              harness.getTenantId(),
              harness.getRegion(),
              Optional.empty(),
              Optional.empty());
      createdIdentities.add(identityName);
      lifecycleIdentityName = identityName;
      lifecyclePolicyMember = harness.toPolicyMember(identityName, identityId);

      // Prime propagation here (not in the benchmark): attach then remove once, retrying the
      // create->attach eventual-consistency window so the timed benchmark never pays for it.
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
  public void teardownBenchmark() throws Exception {
    try {
      if (iamClient != null) {
        try {
          iamClient.removePolicy(
              harness.getPolicyMemberName(),
              harness.getPolicyName(),
              harness.getTenantId(),
              harness.getRegion());
        } catch (Exception e) {
          logger.warn("Failed to remove seed policy in teardown: {}", e.getMessage());
        }

        // Drain every identity minted this run so nothing leaks (mirrors
        // AbstractDocstoreBenchmarkTest). GCP soft-deleted service accounts count against the
        // 100/project quota for 30 days, so a leaked identity is not merely cosmetic.
        for (String identityName : createdIdentities) {
          try {
            iamClient.deleteIdentity(identityName, harness.getTenantId(), harness.getRegion());
          } catch (Exception e) {
            logger.warn("Failed to delete benchmark identity {}: {}", identityName, e.getMessage());
          }
        }
        createdIdentities.clear();

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
   * Attach then remove an inline policy on an already-propagated identity.
   *
   * <p>Times only the two mutating control-plane RPCs. The identity's create and its
   * eventual-consistency wait happen once in {@link #setupBenchmark()}, and its delete in
   * {@link #teardownBenchmark()} — none of that RPC cost bleeds into this measurement, and no
   * {@code Thread.sleep} backoff runs inside the timed region. Reuses {@code getPolicyName()} for
   * both calls because GCP's removePolicy matches on the role translated from the policy actions,
   * not the document name, so attach and remove must name the same role.
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
