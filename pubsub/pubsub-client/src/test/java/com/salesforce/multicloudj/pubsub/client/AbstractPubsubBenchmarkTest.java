package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Abstract JMH benchmark class for PubSub operations */
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractPubsubBenchmarkTest {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractPubsubBenchmarkTest.class);

  protected static final int TINY_MESSAGE = 256; // 256 bytes
  protected static final int SMALL_MESSAGE = 1024; // 1KB
  protected static final int MEDIUM_MESSAGE = 10240; // 10KB
  protected static final int LARGE_MESSAGE = 102400; // 100KB

  protected static final int BATCH_SIZE_SMALL = 10;

  // Pre-population constants — kept small to avoid long setup (each SNS publish ~3-5s round-trip)
  protected static final int PREPOPULATE_RECEIVE = 50;
  protected static final int MESSAGE_AVAILABILITY_DELAY_MS = 1000;

  // @Param for batch ack benchmarks — drives both benchmarkBatchAck and benchmarkLargeBatchAck
  @Param({"1", "10"})
  protected int batchSize;

  protected TopicClient topicClient;
  protected SubscriptionClient subscriptionClient;

  private Harness harness;
  private ExecutorService drainExecutor;

  public interface Harness extends AutoCloseable {
    AbstractTopic<?> createTopic();

    AbstractSubscription<?> createSubscription();

    String getTopicName();

    String getSubscriptionName();
  }

  protected abstract Harness createHarness();

  /**
   * Returns provider ID for result file naming (e.g., "aws-sns", "awssqs", "gcp").
   */
  protected abstract String getProviderId();

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
   * Returns the maximum number of messages allowed in a single batch ack call.
   * AWS: 10 messages per batch, GCP: 1000 messages per batch.
   */
  protected int getMaxBatchAckSize() {
    return BATCH_SIZE_SMALL;
  }

  @Setup(Level.Trial)
  public void setupBenchmark() {
    try {
      logger.info(">>> Setup: Creating harness...");
      harness = createHarness();
      drainExecutor = Executors.newSingleThreadExecutor();

      logger.info(">>> Setup: Creating topic...");
      AbstractTopic<?> topic = harness.createTopic();

      logger.info(">>> Setup: Creating subscription...");
      AbstractSubscription<?> subscription = harness.createSubscription();

      logger.info(">>> Setup: Creating clients...");
      topicClient = new TopicClient(topic);
      subscriptionClient = new SubscriptionClient(subscription);

      // Seed is best-effort and small — SNS/GCP publish is synchronous.
      // Benchmarks are self-sustaining (publish before receive), so a partial seed is fine.
      logger.info(">>> Setup: Seeding {} messages...", PREPOPULATE_RECEIVE);
      try {
        prePopulateMessages(PREPOPULATE_RECEIVE, SMALL_MESSAGE);
      } catch (Exception e) {
        logger.warn(">>> Setup: seed phase failed ({}), continuing anyway", e.getMessage());
      }

      logger.info(">>> Setup: Complete!");

    } catch (Exception e) {
      logger.error(">>> Setup FAILED: {}", e.getMessage(), e);
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() {
    logger.info(">>> Teardown: Draining subscription...");
    drainWithTimeout(2000);

    logger.info(">>> Teardown: Closing resources...");
    closeQuietly(topicClient);
    closeQuietly(subscriptionClient);
    closeQuietly(harness);
    if (drainExecutor != null) {
      drainExecutor.shutdownNow();
    }

    logger.info(">>> Teardown: Complete!");
  }

  private void closeQuietly(AutoCloseable resource) {
    if (resource != null) {
      try {
        resource.close();
      } catch (Exception e) {
        logger.warn("Warning: Failed to close resource: {}", e.getMessage());
      }
    }
  }

  /**
   * Drain up to 10000 messages using a per-receive timeout. When {@code receive()} takes longer
   * than {@code perReceiveTimeoutMs} we infer the queue is empty and stop.
   *
   * <p>Note: neither AWS nor GCP throw on an empty queue — AWS retries with a 250 ms sleep, GCP
   * returns an empty list and retries immediately. Both block the calling thread indefinitely, so
   * we wrap each call in a {@link Future} and cut it off with a timeout.
   */
  protected void drainWithTimeout(long perReceiveTimeoutMs) {
    int drained = 0;
    while (drained < 10000) {
      Future<Message> future = drainExecutor.submit(() -> subscriptionClient.receive());
      try {
        Message msg = future.get(perReceiveTimeoutMs, TimeUnit.MILLISECONDS);
        subscriptionClient.sendAck(msg.getAckID());
        drained++;
      } catch (TimeoutException e) {
        future.cancel(true);
        break;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        future.cancel(true);
        break;
      } catch (ExecutionException e) {
        break;
      }
    }
    logger.info(">>> Drained {} messages", drained);
  }

  /** Single message publish benchmark — 4 concurrent publishers */
  @Benchmark
  @Threads(4)
  public void benchmarkSingleMessagePublish(Blackhole bh) {
    try {
      Message msg = createMessage(SMALL_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark publish FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark single message publish failed", e);
    }
  }

  /**
   * Single message receive-and-ack benchmark — 4 concurrent consumers.
   *
   * <p>Publishes one message before receiving to keep the queue from draining.
   * The publish cost is included in the measurement; use {@link #benchmarkSingleThreadReceive}
   * if you want receive-only latency in isolation.
   *
   * <p>Note: {@code sendAck()} enqueues the ack asynchronously; the underlying RPC cost is NOT
   * captured in this benchmark's latency measurement.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkSingleMessageReceive(Blackhole bh) {
    try {
      topicClient.send(createMessage(SMALL_MESSAGE));
      Message msg = subscriptionClient.receive();
      subscriptionClient.sendAck(msg.getAckID());
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark receive FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark single message receive failed", e);
    }
  }

  /**
   * Single message receive-and-ack benchmark — 1 thread only.
   *
   * <p>Publishes one message before receiving so the queue never drains. Single thread avoids the
   * {@code ReentrantLock} contention present in {@link #benchmarkSingleMessageReceive}.
   *
   * <p>Note: {@code sendAck()} enqueues the ack asynchronously; the underlying RPC cost is NOT
   * captured in this benchmark's latency measurement.
   */
  @Benchmark
  @Threads(1)
  public void benchmarkSingleThreadReceive(Blackhole bh) {
    try {
      topicClient.send(createMessage(SMALL_MESSAGE));
      Message msg = subscriptionClient.receive();
      subscriptionClient.sendAck(msg.getAckID());
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark single thread receive FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark single thread receive failed", e);
    }
  }

  /**
   * Publish-Consume-Ack cycle benchmark.
   *
   * <p>Measures end-to-end latency: publish one message, receive one, ack it.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkPublishConsumeAck(Blackhole bh) {
    try {
      Message msg = createMessage(SMALL_MESSAGE);
      topicClient.send(msg);

      Message received = subscriptionClient.receive();
      bh.consume(received);
      subscriptionClient.sendAck(received.getAckID());
    } catch (Exception e) {
      logger.error(">>> Benchmark E2E FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark publish-consume-ack failed", e);
    }
  }

  /**
   * Sequential (single-threaded) publish benchmark.
   *
   * <p>Measures true single-operation cost without concurrent batching benefits.
   */
  @Benchmark
  @Threads(1)
  public void benchmarkSequentialPublish(Blackhole bh) {
    try {
      Message msg = createMessage(SMALL_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark sequential publish FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark sequential publish failed", e);
    }
  }

  /**
   * Tiny message (256 B) publish benchmark.
   *
   * <p>Tests protocol overhead with minimal payload.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkPublishTinyMessages(Blackhole bh) {
    try {
      Message msg = createMessage(TINY_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark tiny messages FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark tiny messages failed", e);
    }
  }

  /**
   * Medium message (10 KB) publish benchmark.
   *
   * <p>Measures performance with typical medium-sized payloads.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkPublishMediumMessages(Blackhole bh) {
    try {
      Message msg = createMessage(MEDIUM_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark medium messages FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark medium messages failed", e);
    }
  }

  /**
   * Large message (100 KB) publish benchmark.
   *
   * <p>Measures performance near AWS SNS's effective per-message size limit.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkPublishLargeMessages(Blackhole bh) {
    try {
      Message msg = createMessage(LARGE_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark large messages FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark large messages failed", e);
    }
  }

  /**
   * Publish with message attributes/metadata benchmark.
   *
   * <p>Measures overhead of adding metadata for filtering, routing, and tracing.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkPublishWithAttributes(Blackhole bh) {
    try {
      Message msg = createMessageWithAttributes(SMALL_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Benchmark with attributes FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark with attributes failed", e);
    }
  }

  /**
   * Batch acknowledgment benchmark.
   *
   * <p>Publishes {@code batchSize} messages then receives and acks them in a single batch call.
   * Parameterised via {@link #batchSize} ({@code @Param({"1","10"})}).
   *
   * <p>Note: {@code sendAcks()} enqueues the batch ack asynchronously; the underlying RPC cost is
   * NOT captured in this benchmark's latency measurement.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkBatchAck(Blackhole bh) {
    try {
      for (int i = 0; i < batchSize; i++) {
        topicClient.send(createMessage(SMALL_MESSAGE));
      }
      List<AckID> ackIds = new ArrayList<>();
      for (int i = 0; i < batchSize; i++) {
        Message msg = subscriptionClient.receive();
        ackIds.add(msg.getAckID());
        bh.consume(msg);
      }
      subscriptionClient.sendAcks(ackIds);
      bh.consume(ackIds.size());
    } catch (Exception e) {
      logger.error(">>> Benchmark batch ack FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark batch ack failed", e);
    }
  }

  /**
   * Large batch acknowledgment benchmark — shows provider scaling advantage.
   *
   * <p>Publishes then receives {@code Math.min(batchSize, getMaxBatchAckSize())} messages in one
   * batch ack (AWS: 10, GCP: 1000). Parameterised via {@link #batchSize}
   * ({@code @Param({"1","10"})}).
   *
   * <p>Note: {@code sendAcks()} enqueues the batch ack asynchronously; the underlying RPC cost is
   * NOT captured in this benchmark's latency measurement.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkLargeBatchAck(Blackhole bh) {
    int effectiveBatch = Math.min(batchSize, getMaxBatchAckSize());
    try {
      for (int i = 0; i < effectiveBatch; i++) {
        topicClient.send(createMessage(SMALL_MESSAGE));
      }
      List<AckID> ackIds = new ArrayList<>();
      for (int i = 0; i < effectiveBatch; i++) {
        Message msg = subscriptionClient.receive();
        ackIds.add(msg.getAckID());
        bh.consume(msg);
      }
      subscriptionClient.sendAcks(ackIds);
      bh.consume(ackIds.size());
    } catch (Exception e) {
      logger.error(">>> Benchmark large batch ack FAILED: {}", e.getMessage());
      throw new RuntimeException("Benchmark large batch ack failed", e);
    }
  }

  /**
   * High-throughput pipeline benchmark.
   *
   * <p>4 publisher threads and 4 consumer threads run concurrently via JMH {@code @Group} to
   * measure maximum sustainable throughput. Uses 10 s × 3 iterations to allow the pipeline to
   * reach steady state.
   */
  @Benchmark
  @Group("pipeline")
  @GroupThreads(4)
  @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
  public void pipelinePublish(Blackhole bh) {
    try {
      Message msg = createMessage(SMALL_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Pipeline publish FAILED: {}", e.getMessage());
      throw new RuntimeException("Pipeline publish failed", e);
    }
  }

  /**
   * Consumer half of the high-throughput pipeline benchmark.
   *
   * @see #pipelinePublish
   */
  @Benchmark
  @Group("pipeline")
  @GroupThreads(4)
  @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
  public void pipelineConsume(Blackhole bh) {
    try {
      Message msg = subscriptionClient.receive();
      subscriptionClient.sendAck(msg.getAckID());
      bh.consume(msg);
    } catch (Exception e) {
      logger.error(">>> Pipeline consume FAILED: {}", e.getMessage());
      throw new RuntimeException("Pipeline consume failed", e);
    }
  }

  protected Message createMessage(int size) {
    byte[] data = new byte[size];
    ThreadLocalRandom.current().nextBytes(data);
    return Message.builder().withBody(data).build();
  }

  protected Message createMessageWithAttributes(int size) {
    byte[] data = new byte[size];
    ThreadLocalRandom.current().nextBytes(data);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("priority", "high");
    attributes.put("source", "benchmark-test");
    attributes.put("correlationId", "corr-" + System.nanoTime());
    attributes.put("timestamp", String.valueOf(System.currentTimeMillis()));
    attributes.put("region", "us-west-2");
    return Message.builder().withBody(data).withMetadata(attributes).build();
  }

  protected void prePopulateMessages(int count, int messageSize) {
    int published = 0;
    boolean aborted = false;
    for (int i = 0; i < count && !aborted; i++) {
      boolean sent = false;
      for (int attempt = 0; attempt < 3 && !sent; attempt++) {
        try {
          topicClient.send(createMessage(messageSize));
          sent = true;
          published++;
        } catch (Exception e) {
          logger.warn(
              ">>> prePopulate: publish failed (attempt {}/3, msg {}/{}): {}",
              attempt + 1, i + 1, count, e.getMessage());
          if (attempt < 2) {
            try {
              Thread.sleep(1000L << attempt);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              logger.warn(">>> prePopulate: interrupted, aborting seed");
              aborted = true;
              break;
            }
          }
        }
      }
    }
    logger.info(">>> prePopulate: seeded {}/{} messages", published, count);
    try {
      Thread.sleep(MESSAGE_AVAILABILITY_DELAY_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
  public void runBenchmarks() throws RunnerException {
    // JMH forks a new JVM per benchmark group — it doesn't inherit -D flags from Maven/surefire,
    // so we forward PUBSUB_BENCHMARK_* props explicitly. Credentials (AWS_*, GOOGLE_*) flow via
    // OS env automatically and must NOT be forwarded here (JVM args appear in ps and crash logs).
    List<String> forwardedArgs = new ArrayList<>();
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("PUBSUB_BENCHMARK_")) {
        forwardedArgs.add("-D" + key + "=" + System.getProperty(key));
      }
    }

    Options opt =
        new OptionsBuilder()
            .include(".*" + this.getClass().getName() + ".*")
            .forks(1)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-pubsub-results-" + getProviderId() + ".json")
            .jvmArgsAppend(forwardedArgs.toArray(new String[0]))
            .build();

    new Runner(opt).run();
  }
}
