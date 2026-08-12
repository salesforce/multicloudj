package com.salesforce.multicloudj.sts.client;

import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.util.ArrayList;
import java.util.List;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmarks for STS control-plane operations via {@link StsClient}.
 *
 * <p>Control-plane auth is latency-primary: STS throttles aggressively, so throughput mostly
 * measures the provider's rate limiter, not the SDK. The {@code SampleTime} percentiles are the
 * signal that matters here; {@code Throughput} is retained only for trend comparison.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractStsBenchmarkTest {

  // Harness interface
  public interface Harness extends AutoCloseable {
    StsClient createStsClient();

    String getRoleName();

    // May return null if the provider doesn't support web-identity federation.
    String getWebIdentityToken();
  }

  protected Harness harness;
  protected StsClient stsClient;

  protected abstract Harness createHarness();

  protected abstract String getProviderId();

  /**
   * Reads a required config value from OS environment first, then from -D system properties.
   * Fails fast with a clear error if neither is set — avoids silent misconfiguration producing
   * garbage benchmark results.
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
    harness = createHarness();
    stsClient = harness.createStsClient();
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() throws Exception {
    if (harness != null) {
      harness.close();
    }
  }

  /** Caller identity lookup - the most common control-plane read. */
  @Benchmark
  public void benchmarkGetCallerIdentity(Blackhole bh) {
    CallerIdentity identity = stsClient.getCallerIdentity();
    bh.consume(identity);
  }

  /** Assume role and consume the resulting temporary credentials. */
  @Benchmark
  public void benchmarkGetAssumeRoleCredentials(Blackhole bh) {
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole(harness.getRoleName())
            .withSessionName("mcj-benchmark-session")
            .withExpiration(3600)
            .build();
    StsCredentials credentials = stsClient.getAssumeRoleCredentials(request);
    bh.consume(credentials);
  }

  /**
   * Assume role via web identity federation. No-ops when the harness doesn't provide a web
   * identity token, since not every provider supports this flow in a benchmarkable way. JMH's
   * Runner doesn't understand JUnit assumptions, so we skip manually instead of using
   * Assumptions.assumeTrue.
   */
  @Benchmark
  public void benchmarkGetAssumeRoleWithWebIdentity(Blackhole bh) {
    String webIdentityToken = harness.getWebIdentityToken();
    if (webIdentityToken == null) {
      bh.consume(webIdentityToken);
      return;
    }

    AssumeRoleWebIdentityRequest request =
        AssumeRoleWebIdentityRequest.builder()
            .role(harness.getRoleName())
            .webIdentityToken(webIdentityToken)
            .sessionName("mcj-benchmark-web-identity-session")
            .expiration(3600)
            .build();
    StsCredentials credentials = stsClient.getAssumeRoleWithWebIdentityCredentials(request);
    bh.consume(credentials);
  }

  /**
   * Short-lived access/session token issuance. Excluded from the default sweep (see
   * runBenchmarks): AWS GetSessionToken rejects temporary/session credentials, so it cannot run
   * under the assumed-role creds the pipeline uses. Kept for anyone running with long-term
   * IAM-user creds. (Mirrors multicloud-py's supports_get_access_token capability gate.)
   */
  @Benchmark
  public void benchmarkGetAccessToken(Blackhole bh) {
    GetAccessTokenRequest request =
        GetAccessTokenRequest.newBuilder().withDurationSeconds(3600).build();
    StsCredentials credentials = stsClient.getAccessToken(request);
    bh.consume(credentials);
  }

  /** Concurrent assume-role calls to surface refresh-storm/throttling behavior. */
  @Benchmark
  @Threads(8)
  public void benchmarkConcurrentAssumeRole(Blackhole bh) {
    AssumedRoleRequest request =
        AssumedRoleRequest.newBuilder()
            .withRole(harness.getRoleName())
            .withSessionName("mcj-benchmark-concurrent-session")
            .withExpiration(3600)
            .build();
    StsCredentials credentials = stsClient.getAssumeRoleCredentials(request);
    bh.consume(credentials);
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("STS_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    Options opt =
        new OptionsBuilder()
            .include(".*" + this.getClass().getName() + ".*")
            // GetSessionToken can't be called with session creds; unexercisable in the pipeline.
            .exclude(".*benchmarkGetAccessToken.*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-sts-results-" + getProviderId() + ".json")
            .jvmArgsAppend(forwardedArgs.toArray(new String[0]))
            .build();

    new Runner(opt).run();
  }
}
