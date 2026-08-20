package com.salesforce.multicloudj.registry.client;

import com.salesforce.multicloudj.registry.model.Image;
import java.io.IOException;
import java.io.InputStream;
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
 * JMH benchmarks for container-registry data-plane operations via {@link ContainerRegistryClient}.
 *
 * <p>Registry pulls are data-plane: both throughput (bandwidth/decompress rate) and tail latency
 * are honest signals, so this suite runs {@code Throughput} + {@code SampleTime}.
 */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractRegistryBenchmarkTest {

  private static final int COPY_BUFFER_SIZE = 8192;

  protected ContainerRegistryClient client;
  private String smallImageRef;
  private String multiArchImageRef;

  // Harness interface
  public interface Harness extends AutoCloseable {
    ContainerRegistryClient createClient();

    String getSmallImageRef();

    // May return null if the provider has no multi-arch test image available.
    String getMultiArchImageRef();

    // Whether a multi-arch test image is configured; drives the benchmarkPullMultiArch exclude.
    default boolean supportsMultiArch() {
      return getMultiArchImageRef() != null;
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
   * Returns null when unset (used for the optional multi-arch image ref).
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
    harness = createHarness();
    client = harness.createClient();
    smallImageRef = harness.getSmallImageRef();
    multiArchImageRef = harness.getMultiArchImageRef();
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() throws Exception {
    if (client != null) {
      client.close();
    }
    if (harness != null) {
      harness.close();
    }
  }

  /**
   * Manifest fetch + parse latency for a small single-arch image.
   *
   * <p>Caveat: re-pulls the same {@code smallImageRef} on every invocation, so the registry's edge
   * cache and the warm {@code OciHttpTransport} connection pool make this a best-case (cache-hit)
   * latency, not a cold pull.
   */
  @Benchmark
  public void benchmarkPullManifest(Blackhole bh) {
    Image image = client.pull(smallImageRef);
    bh.consume(image.getDigest());
  }

  /**
   * Multi-arch pull, exercising selectPlatformFromIndex plus the second manifest fetch. Excluded by
   * {@link #runBenchmarks()} when the provider has no multi-arch fixture, so this never publishes a
   * datapoint for work that didn't run.
   */
  @Benchmark
  public void benchmarkPullMultiArch(Blackhole bh) {
    Image image = client.pull(multiArchImageRef);
    bh.consume(image.getDigest());
  }

  /**
   * End-to-end pull + extract, the realistic full-image-materialization path.
   *
   * <p>Same cache-hit caveat as {@link #benchmarkPullManifest} applies to the pull half.
   */
  @Benchmark
  public void benchmarkPullAndExtract(Blackhole bh) {
    Image image = client.pull(smallImageRef);
    try (InputStream tar = client.extract(image)) {
      bh.consume(drain(tar));
    } catch (IOException e) {
      throw new RuntimeException("Benchmark pull and extract failed", e);
    }
  }

  private long drain(InputStream in) throws IOException {
    byte[] buffer = new byte[COPY_BUFFER_SIZE];
    long total = 0;
    int read;
    while ((read = in.read(buffer)) != -1) {
      total += read;
    }
    return total;
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("REGISTRY_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    OptionsBuilder builder = new OptionsBuilder();
    builder
        .include(".*" + this.getClass().getName() + ".*")
        .forks(1)
        .resultFormat(ResultFormatType.JSON)
        .result("target/jmh-registry-results-" + getProviderId() + ".json")
        .jvmArgsAppend(forwardedArgs.toArray(new String[0]));

    // Skip the multi-arch benchmark on providers without a multi-arch fixture, rather than
    // publishing an empty-method datapoint under the name of a real pull.
    if (!createHarness().supportsMultiArch()) {
      builder.exclude(".*benchmarkPullMultiArch.*");
    }

    new Runner(builder.build()).run();
  }
}
