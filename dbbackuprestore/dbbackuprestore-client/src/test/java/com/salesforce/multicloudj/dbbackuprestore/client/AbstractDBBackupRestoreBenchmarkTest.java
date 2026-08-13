package com.salesforce.multicloudj.dbbackuprestore.client;

import com.salesforce.multicloudj.dbbackuprestore.driver.Backup;
import com.salesforce.multicloudj.dbbackuprestore.driver.Restore;
import com.salesforce.multicloudj.dbbackuprestore.driver.RestoreRequest;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmarks for DB backup/restore control-plane operations via {@link DBBackupRestoreClient}.
 *
 * <p>Backup/restore admin APIs are latency-primary and low-QPS: throughput mostly reflects the
 * provider's rate limiter, so {@code SampleTime} percentiles are the signal and {@code Throughput}
 * is trend-only.
 *
 * <p>Restore is kickoff-only: {@code restoreBackup} initiates an async job and there is no delete
 * API, so a restored table/database is left behind permanently. {@code benchmarkRestoreKickoff}
 * must never run in a warmup/measurement loop; only the read-only methods are safe to sweep.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDBBackupRestoreBenchmarkTest {

  protected DBBackupRestoreClient client;
  protected String backupId;
  protected String restoreId;

  // Harness interface
  public interface Harness extends AutoCloseable {
    DBBackupRestoreClient createClient();

    String getTargetResource();

    default String getKnownBackupId() {
      return null;
    }

    /**
     * A statically-known restore ID to exercise {@link #benchmarkGetRestoreJob(Blackhole)}, read
     * from {@code DBBACKUPRESTORE_BENCHMARK_RESTORE_ID}. When unset, setup does NOT create one
     * (that would spawn an uncleaned-up restore job on every trial, since {@code @Setup} runs once
     * per fork per benchmark method and there is no delete API), and the get-restore-job benchmark
     * is dropped from the sweep rather than timing an empty method.
     */
    default String getKnownRestoreId() {
      return optionalEnv("DBBACKUPRESTORE_BENCHMARK_RESTORE_ID");
    }

    default String getRoleId() {
      return null;
    }

    default String getVaultId() {
      return null;
    }

    default String getKmsEncryptionKeyId() {
      return null;
    }

    /**
     * Opt-in switch for the destructive {@link #benchmarkRestoreKickoff(Blackhole)}. Defaults to
     * the {@code DBBACKUPRESTORE_BENCHMARK_ALLOW_DESTRUCTIVE} flag so an operator with a disposable
     * target can enable it explicitly; false everywhere else.
     */
    default boolean isDestructiveEnabled() {
      return Boolean.parseBoolean(optionalEnv("DBBACKUPRESTORE_BENCHMARK_ALLOW_DESTRUCTIVE"));
    }
  }

  protected Harness harness;

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

  /**
   * Reads an optional config value from OS environment first, then from -D system properties.
   * Returns null when unset (used for the optional role/vault/KMS/restore-id inputs).
   */
  protected static String optionalEnv(String name) {
    String value = System.getenv(name);
    if (StringUtils.isBlank(value)) {
      value = System.getProperty(name);
    }
    return StringUtils.isBlank(value) ? null : value;
  }

  @Setup(Level.Trial)
  public void setupBenchmark() throws Exception {
    try {
      harness = createHarness();
      client = harness.createClient();

      backupId = harness.getKnownBackupId();
      if (backupId == null || backupId.isEmpty()) {
        List<Backup> backups = client.listBackups();
        if (backups.isEmpty()) {
          throw new IllegalStateException(
              "No backups available for resource; cannot set up DBBackupRestore benchmark");
        }
        backupId = backups.get(0).getBackupId();
      }

      // Intentionally do NOT kick off a real restore here. setup runs once per fork per benchmark
      // method, so a fallback restore would spawn an uncleaned-up restore job on every trial (there
      // is no delete API). benchmarkGetRestoreJob null-guards itself when no known restore ID is
      // provided; supply one via Harness.getKnownRestoreId() to exercise that path.
      restoreId = harness.getKnownRestoreId();
    } catch (Exception e) {
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  // DBBackupRestoreClient exposes no delete API, so any restored table/database created by
  // benchmarkRestoreKickoff or setupBenchmark's fallback restore is left behind permanently.
  @TearDown(Level.Trial)
  public void teardownBenchmark() throws Exception {
    try {
      if (client != null) {
        client.close();
      }

      if (harness != null) {
        harness.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error closing harness", e);
    }
  }

  private RestoreRequest buildRestoreRequest() {
    RestoreRequest.RestoreRequestBuilder requestBuilder =
        RestoreRequest.builder().backupId(backupId).targetResource(harness.getTargetResource());

    if (harness.getRoleId() != null) {
      requestBuilder.roleId(harness.getRoleId());
    }
    if (harness.getVaultId() != null) {
      requestBuilder.vaultId(harness.getVaultId());
    }
    if (harness.getKmsEncryptionKeyId() != null) {
      requestBuilder.kmsEncryptionKeyId(harness.getKmsEncryptionKeyId());
    }

    return requestBuilder.build();
  }

  /** List Backups - list + pagination, the most common admin read. */
  @Benchmark
  public void benchmarkListBackups(Blackhole bh) {
    try {
      List<Backup> backups = client.listBackups();
      bh.consume(backups);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark list backups failed", e);
    }
  }

  /** Get Backup - single metadata fetch. */
  @Benchmark
  public void benchmarkGetBackup(Blackhole bh) {
    try {
      Backup backup = client.getBackup(backupId);
      bh.consume(backup);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get backup failed", e);
    }
  }

  /**
   * Restore Kickoff. Measures the latency of the API call that INITIATES a restore, not the time
   * the restore itself takes (restores run async on the provider side and can take minutes to
   * hours). Excluded from the default sweep in {@link #runBenchmarks()} since each invocation
   * leaves an uncleaned-up restore; kept as a {@code @Benchmark} so an operator with a disposable
   * target can run it explicitly via {@code DBBACKUPRESTORE_BENCHMARK_ALLOW_DESTRUCTIVE=true}. The
   * in-method destructive guard is the real fence — the {@code .exclude()} in the launcher only
   * covers callers that reuse it.
   */
  @Benchmark
  public void benchmarkRestoreKickoff(Blackhole bh) {
    // Hard guard, not just a launcher .exclude(): every invocation kicks off a real restore with no
    // delete API to clean it up. Refuse unless the operator explicitly opts in, so a runner that
    // builds its own Options (or the unconditional generated BenchmarkList) can't storm the target.
    if (!harness.isDestructiveEnabled()) {
      throw new IllegalStateException(
          "benchmarkRestoreKickoff is destructive (creates an uncleaned-up restore per "
              + "invocation); set DBBACKUPRESTORE_BENCHMARK_ALLOW_DESTRUCTIVE=true to run it");
    }
    try {
      String newRestoreId = client.restoreBackup(buildRestoreRequest());
      bh.consume(newRestoreId);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark restore kickoff failed", e);
    }
  }

  /** Get Restore Job - the status-poll read that runs repeatedly during a restore. */
  @Benchmark
  public void benchmarkGetRestoreJob(Blackhole bh) {
    // No restore ID is available unless the harness supplies one via getKnownRestoreId(); setup
    // deliberately does not create one. Skip cheaply rather than NPE so the default read-only
    // trial stays green.
    if (restoreId == null || restoreId.isEmpty()) {
      bh.consume(restoreId);
      return;
    }
    try {
      Restore restore = client.getRestoreJob(restoreId);
      bh.consume(restore);
    } catch (Exception e) {
      throw new RuntimeException("Benchmark get restore job failed", e);
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("DBBACKUPRESTORE_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    // Capability-driven exclusions: what gets swept depends on what the harness can actually
    // exercise, rather than a fixed regex.
    Harness capabilities = createHarness();

    OptionsBuilder builder = new OptionsBuilder();
    builder.include(".*" + this.getClass().getName() + ".*");

    // benchmarkRestoreKickoff kicks off a real, uncleaned-up restore on every invocation, so it
    // must never run in a warmup/measurement loop unless the operator explicitly opts in.
    if (!capabilities.isDestructiveEnabled()) {
      builder.exclude(".*benchmarkRestoreKickoff.*");
    }

    // benchmarkGetRestoreJob needs a known restore ID; with none configured it would only time an
    // empty method and publish a meaningless throughput number. Drop it from the sweep instead.
    if (StringUtils.isBlank(capabilities.getKnownRestoreId())) {
      builder.exclude(".*benchmarkGetRestoreJob.*");
    }

    Options opt =
        builder
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-dbbackuprestore-results-" + getProviderId() + ".json")
            .jvmArgsAppend(forwardedArgs.toArray(new String[0]))
            .build();

    new Runner(opt).run();
  }
}
