package com.salesforce.multicloudj.pubsub.client;

import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.pubsub.driver.AckID;
import com.salesforce.multicloudj.pubsub.driver.Message;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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

/** Abstract JMH benchmark class for PubSub operations */
@Disabled
@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractPubsubBenchmarkTest {

  // Message size constants
  protected static final int TINY_MESSAGE = 256; // 256 bytes
  protected static final int SMALL_MESSAGE = 1024; // 1KB
  protected static final int MEDIUM_MESSAGE = 10240; // 10KB
  protected static final int LARGE_MESSAGE = 102400; // 100KB

  // Batch size constants
  protected static final int BATCH_SIZE_SMALL = 10;
  protected static final int BATCH_SIZE_MEDIUM = 50;
  protected static final int BATCH_SIZE_LARGE = 100;

  // Benchmark iteration and pre-population constants
  protected static final int ITERATIONS_PER_BENCHMARK = 10; // Operations per benchmark iteration
  protected static final int ITERATIONS_NACK_BENCHMARK = 5; // Reduced for NACK (redelivery overhead)
  protected static final int PREPOPULATE_SMALL = 100; // Small pre-population for basic benchmarks
  protected static final int PREPOPULATE_BATCH = 200; // For batch ack benchmarks
  protected static final int PREPOPULATE_HIGH_THROUGHPUT = 500; // For high throughput tests
  protected static final int PREPOPULATE_BACKLOG = 5000; // Large backlog simulation
  protected static final int PROPAGATION_DELAY_MS = 10000; // Wait for messages to propagate (10s)
  protected static final int MESSAGE_AVAILABILITY_DELAY_MS = 1000; // Wait for message availability

  // Test infrastructure
  protected TopicClient topicClient;
  protected SubscriptionClient subscriptionClient;
  protected Random random;

  // Flags to track pre-population (per benchmark)
  private volatile boolean messagesPrePopulatedForReceive = false;
  private volatile boolean messagesPrePopulatedForE2E = false;
  private volatile boolean messagesPrePopulatedForBacklog = false;
  private volatile boolean messagesPrePopulatedForBatchAck = false;
  private volatile boolean messagesPrePopulatedForLargeBatchAck = false;
  private volatile boolean messagesPrePopulatedForHighThroughput = false;

  // Harness interface for resource management
  public interface Harness extends AutoCloseable {
    AbstractTopic<?> createTopic();

    AbstractSubscription<?> createSubscription();

    String getTopicName();

    String getSubscriptionName();
  }

  protected abstract Harness createHarness();

  /**
   * Returns provider ID for profiler output directory naming (e.g., "aws-sns", "aws-sqs", "gcp")
   */
  protected abstract String getProviderId();

  /**
   * Returns maximum batch ack size for this provider.
   * AWS: 10 messages per batch, GCP: 1000 messages per batch
   */
  protected int getMaxBatchAckSize() {
    return BATCH_SIZE_SMALL; // Default to 10 (AWS limit)
  }

  private Harness harness;

  @Setup(Level.Trial)
  public void setupBenchmark() {
    try {
      System.out.println(">>> Setup: Creating harness...");
      harness = createHarness();
      random = new Random(42);

      System.out.println(">>> Setup: Creating topic...");
      AbstractTopic<?> topic = harness.createTopic();

      System.out.println(">>> Setup: Creating subscription...");
      AbstractSubscription<?> subscription = harness.createSubscription();

      System.out.println(">>> Setup: Creating clients...");
      topicClient = new TopicClient(topic);
      subscriptionClient = new SubscriptionClient(subscription);

      // DISABLED: drainSubscription blocks forever on empty queues (no timeout on receive)
      // We'll drain in teardown instead, after benchmarks have populated messages
      System.out.println(">>> Setup: Complete!");

    } catch (Exception e) {
      System.err.println(">>> Setup FAILED: " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException("Failed to setup benchmark", e);
    }
  }

  @TearDown(Level.Trial)
  public void teardownBenchmark() {
    System.out.println(">>> Teardown: Starting cleanup...");

    // DISABLED: drainSubscription blocks forever - skipping for now
    // TODO: Implement drain with timeout or limited attempts
    System.out.println(">>> Teardown: Skipping drain (would block)...");

    // Close resources (always attempt all, even if some fail)
    System.out.println(">>> Teardown: Closing resources...");
    closeQuietly(topicClient);
    closeQuietly(subscriptionClient);
    closeQuietly(harness);

    // Reset pre-population flags
    messagesPrePopulatedForReceive = false;
    messagesPrePopulatedForE2E = false;
    messagesPrePopulatedForBacklog = false;
    messagesPrePopulatedForBatchAck = false;
    messagesPrePopulatedForLargeBatchAck = false;
    messagesPrePopulatedForHighThroughput = false;

    System.out.println(">>> Teardown: Complete!");
  }

  /** Close resource without throwing exceptions */
  private void closeQuietly(AutoCloseable resource) {
    if (resource != null) {
      try {
        resource.close();
      } catch (Exception e) {
        System.err.println("Warning: Failed to close resource: " + e.getMessage());
      }
    }
  }

  // ============ BENCHMARK METHODS ============

  /** Single message publish benchmark */
  @Benchmark
  @Threads(4)
  public void benchmarkSingleMessagePublish(Blackhole bh) {
    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        Message msg = createMessage(SMALL_MESSAGE);
        topicClient.send(msg);
        bh.consume(msg);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark publish FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark single message publish failed", e);
    }
  }

  /**
   * Single message receive and ack benchmark with continuous replenishment.
   *
   * This benchmark maintains steady state by publishing messages while receiving.
   * Pattern: publish-then-receive ensures queue never runs empty.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkSingleMessageReceive(Blackhole bh) {
    // Pre-populate messages once per benchmark run
    ensurePrePopulated(
        () -> messagesPrePopulatedForReceive,
        () -> messagesPrePopulatedForReceive = true,
        "Receive",
        PREPOPULATE_SMALL,
        SMALL_MESSAGE);

    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        // Publish one to replenish the queue (for future iterations)
        Message msgToPublish = createMessage(SMALL_MESSAGE);
        topicClient.send(msgToPublish);

        // Receive and ack one message (from pre-populated or previously published)
        Message msgReceived = subscriptionClient.receive();
        subscriptionClient.sendAck(msgReceived.getAckID());
        bh.consume(msgReceived);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark receive FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark single message receive failed", e);
    }
  }

  /**
   * Publish-Consume-Ack cycle benchmark with continuous replenishment.
   *
   * Pre-populates messages once to ensure there's always something to receive.
   * Each iteration: publishes a new message (for future), receives one, and acks it.
   * This measures end-to-end latency with continuous replenishment maintaining steady state.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkPublishConsumeAck(Blackhole bh) {
    // Pre-populate messages once per benchmark run (thread-safe)
    ensurePrePopulated(
        () -> messagesPrePopulatedForE2E,
        () -> messagesPrePopulatedForE2E = true,
        "E2E",
        PREPOPULATE_SMALL,
        SMALL_MESSAGE);

    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        // Publish a new message (replenishes for future iterations)
        Message msg = createMessage(SMALL_MESSAGE);
        topicClient.send(msg);

        // Receive a message (from previously published messages)
        Message received = subscriptionClient.receive();
        bh.consume(received);

        // Ack
        subscriptionClient.sendAck(received.getAckID());
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark E2E FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark publish-consume-ack failed", e);
    }
  }

  /**
   * Sequential (single-threaded) publish benchmark.
   *
   * Measures true single-operation cost without concurrent batching benefits.
   * This is the baseline for understanding per-operation overhead.
   */
  @Benchmark
  @Threads(1)
  public void benchmarkSequentialPublish(Blackhole bh) {
    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        Message msg = createMessage(SMALL_MESSAGE);
        topicClient.send(msg);
        bh.consume(msg);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark sequential publish FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark sequential publish failed", e);
    }
  }

  /**
   * Tiny message (256B) publish benchmark.
   *
   * Measures performance with minimal payload - tests protocol overhead.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkPublishTinyMessages(Blackhole bh) {
    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        Message msg = createMessage(TINY_MESSAGE);
        topicClient.send(msg);
        bh.consume(msg);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark tiny messages FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark tiny messages failed", e);
    }
  }

  /**
   * Medium message (10KB) publish benchmark.
   *
   * Measures performance with typical medium-sized payloads.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkPublishMediumMessages(Blackhole bh) {
    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        Message msg = createMessage(MEDIUM_MESSAGE);
        topicClient.send(msg);
        bh.consume(msg);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark medium messages FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark medium messages failed", e);
    }
  }

  /**
   * Large message (100KB) publish benchmark.
   *
   * Measures performance with large payloads - near AWS limit.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkPublishLargeMessages(Blackhole bh) {
    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        Message msg = createMessage(LARGE_MESSAGE);
        topicClient.send(msg);
        bh.consume(msg);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark large messages FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark large messages failed", e);
    }
  }

  /**
   * Publish with message attributes/metadata benchmark.
   *
   * Measures overhead of adding metadata for filtering, routing, and tracing.
   * Common production pattern with 5-10% expected overhead.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkPublishWithAttributes(Blackhole bh) {
    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        Message msg = createMessageWithAttributes(SMALL_MESSAGE);
        topicClient.send(msg);
        bh.consume(msg);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark with attributes FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark with attributes failed", e);
    }
  }

  /**
   * Max size message publish benchmark.
   *
   * Tests performance with large messages (100KB).
   * Note: Uses LARGE_MESSAGE (100KB) instead of theoretical max (256KB) because
   * AWS SNS's 256KB limit includes payload + attributes + API envelope overhead.
   */
  @Benchmark
  @Threads(2)
  public void benchmarkMaxSizeMessages(Blackhole bh) {
    try {
      // Use LARGE_MESSAGE (100KB) to stay well within AWS SNS's 256KB batch limit
      // AWS limit applies to entire request: payload + attributes + JSON structure
      Message msg = createMessage(LARGE_MESSAGE);
      topicClient.send(msg);
      bh.consume(msg);
    } catch (Exception e) {
      System.err.println(">>> Benchmark max size FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark max size failed", e);
    }
  }

  /**
   * Message backlog receive benchmark.
   *
   * Measures performance when consuming from a queue with significant backlog.
   * Simulates recovery scenario after downtime or traffic spike.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkMessageBacklog(Blackhole bh) {
    // Pre-populate large backlog once per benchmark run
    ensurePrePopulated(
        () -> messagesPrePopulatedForBacklog,
        () -> messagesPrePopulatedForBacklog = true,
        "Backlog",
        PREPOPULATE_BACKLOG,
        SMALL_MESSAGE);

    try {
      for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
        // Publish one to replenish (continuous replenishment)
        Message msgToPublish = createMessage(SMALL_MESSAGE);
        topicClient.send(msgToPublish);

        // Receive and ack one message from backlog
        Message msgReceived = subscriptionClient.receive();
        subscriptionClient.sendAck(msgReceived.getAckID());
        bh.consume(msgReceived);
      }
    } catch (Exception e) {
      System.err.println(">>> Benchmark backlog FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark backlog failed", e);
    }
  }

  /**
   * Batch acknowledgment benchmark.
   *
   * Measures efficiency of batch ack - critical optimization that reduces API calls by 10x.
   * Receives 10 messages, collects AckIDs, then sends batch ack.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkBatchAck(Blackhole bh) {
    // Pre-populate messages once per benchmark run
    ensurePrePopulated(
        () -> messagesPrePopulatedForBatchAck,
        () -> messagesPrePopulatedForBatchAck = true,
        "Batch ack",
        PREPOPULATE_BATCH,
        SMALL_MESSAGE);

    try {
      // Publish 10 to replenish (continuous replenishment)
      for (int i = 0; i < BATCH_SIZE_SMALL; i++) {
        Message msgToPublish = createMessage(SMALL_MESSAGE);
        topicClient.send(msgToPublish);
      }

      // Receive 10 messages and collect AckIDs
      List<AckID> ackIds = new ArrayList<>();
      for (int i = 0; i < BATCH_SIZE_SMALL; i++) {
        Message msg = subscriptionClient.receive();
        ackIds.add(msg.getAckID());
        bh.consume(msg);
      }

      // Batch ack all at once
      subscriptionClient.sendAcks(ackIds);
      bh.consume(ackIds.size());

    } catch (Exception e) {
      System.err.println(">>> Benchmark batch ack FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark batch ack failed", e);
    }
  }

  /**
   * Large batch acknowledgment benchmark.
   *
   * Measures provider scaling advantages at batch limits.
   * AWS: 10 messages max, GCP: 1000 messages max.
   * Shows GCP's 100x batch advantage.
   */
  @Benchmark
  @Threads(4)
  public void benchmarkLargeBatchAck(Blackhole bh) {
    // Pre-populate large number of messages once per benchmark run
    // Cap test batch size for fair comparison and avoid OOM
    // AWS: min(10, 100) = 10, GCP: min(1000, 100) = 100
    int testBatchSize = Math.min(getMaxBatchAckSize(), BATCH_SIZE_LARGE);
    int prePopulateCount = testBatchSize * 5; // 5 iterations worth
    ensurePrePopulated(
        () -> messagesPrePopulatedForLargeBatchAck,
        () -> messagesPrePopulatedForLargeBatchAck = true,
        "Large batch ack",
        prePopulateCount,
        SMALL_MESSAGE);

    try {
      // Publish batch to replenish (continuous replenishment)
      for (int i = 0; i < testBatchSize; i++) {
        Message msgToPublish = createMessage(SMALL_MESSAGE);
        topicClient.send(msgToPublish);
      }

      // Receive large batch and collect AckIDs
      List<AckID> ackIds = new ArrayList<>();
      for (int i = 0; i < testBatchSize; i++) {
        Message msg = subscriptionClient.receive();
        ackIds.add(msg.getAckID());
        bh.consume(msg);
      }

      // Batch ack all at once
      subscriptionClient.sendAcks(ackIds);
      bh.consume(ackIds.size());

    } catch (Exception e) {
      System.err.println(">>> Benchmark large batch ack FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark large batch ack failed", e);
    }
  }

  /**
   * High throughput pipeline benchmark.
   *
   * Measures maximum sustainable throughput under load.
   * 8 threads (4 pub + 4 sub pattern), reveals throttling limits and system capacity.
   *
   * NOTE: Previously failed on GCP evening run with socket errors in Sampling mode.
   * Re-enabled to test at different time. If fails again, reduce threads to 4.
   */
  @Benchmark
  @Threads(8)
  public void benchmarkHighThroughputPipeline(Blackhole bh) {
    // Pre-populate messages once per benchmark run
    ensurePrePopulated(
        () -> messagesPrePopulatedForHighThroughput,
        () -> messagesPrePopulatedForHighThroughput = true,
        "High throughput",
        PREPOPULATE_HIGH_THROUGHPUT,
        SMALL_MESSAGE);

    try {
      // Split work: half threads publish, half consume
      // Thread-safe random assignment
      boolean shouldPublish = (Thread.currentThread().getId() % 2 == 0);

      if (shouldPublish) {
        // Publishing threads
        for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
          Message msg = createMessage(SMALL_MESSAGE);
          topicClient.send(msg);
          bh.consume(msg);
        }
      } else {
        // Consuming threads
        for (int i = 0; i < ITERATIONS_PER_BENCHMARK; i++) {
          Message msg = subscriptionClient.receive();
          subscriptionClient.sendAck(msg.getAckID());
          bh.consume(msg);
        }
      }

    } catch (Exception e) {
      System.err.println(">>> Benchmark high throughput FAILED: " + e.getMessage());
      throw new RuntimeException("Benchmark high throughput failed", e);
    }
  }

  // ============ UTILITY METHODS ============

  /** Create a message of specified size with random data */
  protected Message createMessage(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    return Message.builder().withBody(data).build();
  }

  /** Create a message with timestamp for latency measurement */
  protected Message createTimestampedMessage(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    long timestamp = System.nanoTime();
    Map<String, String> attributes = new HashMap<>();
    attributes.put("timestamp", String.valueOf(timestamp));
    return Message.builder().withBody(data).withMetadata(attributes).build();
  }

  /** Create a message with common production attributes for filtering/routing/tracing */
  protected Message createMessageWithAttributes(int size) {
    byte[] data = new byte[size];
    random.nextBytes(data);
    Map<String, String> attributes = new HashMap<>();
    attributes.put("priority", "high");
    attributes.put("source", "benchmark-test");
    attributes.put("correlationId", "corr-" + System.nanoTime());
    attributes.put("timestamp", String.valueOf(System.currentTimeMillis()));
    attributes.put("region", "us-west-2");
    return Message.builder().withBody(data).withMetadata(attributes).build();
  }

  /**
   * Ensures messages are pre-populated exactly once using double-checked locking.
   * Reduces code duplication across benchmarks.
   *
   * @param isPopulated Supplier that checks if already populated
   * @param markPopulated Runnable that marks as populated
   * @param benchmarkName Name of the benchmark (for logging)
   * @param count Number of messages to pre-populate
   * @param messageSize Size of each message in bytes
   */
  private void ensurePrePopulated(
      java.util.function.BooleanSupplier isPopulated,
      Runnable markPopulated,
      String benchmarkName,
      int count,
      int messageSize) {
    if (!isPopulated.getAsBoolean()) {
      synchronized (this) {
        if (!isPopulated.getAsBoolean()) {
          System.out.println(
              ">>> " + benchmarkName + " benchmark: Pre-populating " + count + " messages...");
          prePopulateMessages(count, messageSize);
          System.out.println(
              ">>> "
                  + benchmarkName
                  + " benchmark: Complete, waiting for propagation...");
          try {
            Thread.sleep(PROPAGATION_DELAY_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          markPopulated.run();
          System.out.println(">>> " + benchmarkName + " benchmark: Ready!");
        }
      }
    }
  }

  /** Pre-populate messages for receive benchmarks */
  protected void prePopulateMessages(int count, int messageSize) {
    try {
      for (int i = 0; i < count; i++) {
        Message msg = createMessage(messageSize);
        topicClient.send(msg);
      }
      // Wait for messages to be available
      Thread.sleep(MESSAGE_AVAILABILITY_DELAY_MS);
    } catch (Exception e) {
      throw new RuntimeException("Failed to pre-populate messages", e);
    }
  }

  /** Drain all messages from subscription */
  protected void drainSubscription() {
    try {
      int drained = 0;
      while (drained < 1000) { // Safety limit
        Message msg = subscriptionClient.receive();
        subscriptionClient.sendAck(msg.getAckID());
        drained++;
      }
    } catch (Exception e) {
      // Expected when queue is empty
    }
  }

  /** JUnit test method to run JMH benchmarks */
  @Test
  public void runBenchmarks() throws RunnerException {
    Options opt =
        new OptionsBuilder()
            .include(".*" + this.getClass().getName() + ".*")
            .forks(1)
            .warmupIterations(3)
            .measurementIterations(5)
            .resultFormat(ResultFormatType.JSON)
            .result("target/jmh-pubsub-results-" + getProviderId() + ".json")
            .build();

    new Runner(opt).run();
  }

}
