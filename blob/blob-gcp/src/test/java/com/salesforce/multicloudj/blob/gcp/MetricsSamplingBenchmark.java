package com.salesforce.multicloudj.blob.gcp;

import com.salesforce.multicloudj.common.observability.ConnectionPoolMetrics;
import com.salesforce.multicloudj.common.observability.Metric;
import com.salesforce.multicloudj.common.observability.MetricsPublisher;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.http.pool.PoolStats;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Measures the per-response CPU cost that PR #532's connection-pool sampling adds to the GCP
 * request path, in isolation from the network.
 *
 * <p>The overhead question for #532 cannot be answered by a live-upload wall-clock comparison: each
 * upload is ~100-200 ms of network round-trip with tens of ms of jitter, while the sampling work is
 * a handful of getter reads plus a small list allocation — so the network dominates the signal by
 * four-to-five orders of magnitude and swamps any measurement. This microbenchmark instead
 * exercises the exact production path with no I/O, so JMH can report a stable ns/op figure with a
 * confidence interval.
 *
 * <p>What is measured is the real path, not a stand-in: {@link
 * GcpConnectionPoolMetricsInterceptor#process} reading the pool statistics via its {@code
 * PoolStatsSupplier}, translating them through {@link ConnectionPoolMetrics#from} (four {@link
 * Metric} allocations plus the backing list), and handing the result to the publisher. The
 * publisher is a no-op sink that forwards to a {@link Blackhole} so that JMH cannot
 * dead-code-eliminate the work while the benchmark still measures sampling cost rather than any
 * real sink's cost. The {@link PoolStats} value models a saturated pool (max=4, leased=4,
 * available=0, pending=20) — the same shape the accuracy probe observed — so the measured path is
 * representative of load.
 *
 * <p>Run standalone (recommended, clean JMH lifecycle):
 * <pre>
 *   mvn -q test-compile -pl blob/blob-gcp
 *   mvn -q exec:java -pl blob/blob-gcp \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=com.salesforce.multicloudj.blob.gcp.MetricsSamplingBenchmark
 * </pre>
 * or invoke {@link #main} from an IDE. Reports average time per {@code process(...)} call.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@State(Scope.Thread)
public class MetricsSamplingBenchmark {

  /** No-op publisher that pushes into a Blackhole so the sampled metrics are not optimised away. */
  private static final class SinkPublisher implements MetricsPublisher {
    private Blackhole blackhole;

    @Override
    public void publish(List<Metric> metrics) {
      blackhole.consume(metrics);
    }
  }

  private GcpConnectionPoolMetricsInterceptor interceptor;
  private SinkPublisher publisher;

  @Setup(Level.Trial)
  public void setup() {
    publisher = new SinkPublisher();
    // Production wires `connectionManager::getTotalStats`, which allocates a fresh PoolStats on
    // every call (httpcore 4.x computes stats on demand). Model that allocation here so the
    // measured cost is not understated: a saturated-pool snapshot PoolStats(leased, pending,
    // available, max) constructed per invocation, the same shape the accuracy probe observed.
    interceptor =
        new GcpConnectionPoolMetricsInterceptor(() -> new PoolStats(4, 20, 0, 4), publisher);
  }

  /**
   * The full per-response sampling path: read pool stats -> {@link ConnectionPoolMetrics#from} ->
   * publish. {@code response}/{@code context} are unused by the interceptor, so null is passed.
   */
  @Benchmark
  public void sampleAndPublish(Blackhole bh) {
    publisher.blackhole = bh;
    interceptor.process(null, null);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(MetricsSamplingBenchmark.class.getSimpleName())
            .build();
    new Runner(opt).run();
  }
}
