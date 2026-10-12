package com.salesforce.multicloudj.pubsub.driver;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.GetAttributeResult;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Deterministic regression tests for the {@code receive()} lost-wakeup deadlock.
 *
 * <p>The prefetch worker signals {@code batchArrived} and releases the lock inside {@code
 * doPrefetch}'s critical section, but the buggy code clears {@code prefetchInFlight} in an outer
 * {@code finally} that runs after the lock is released. A signalled {@code receive()} can
 * re-acquire the lock in that window, observe the still-current {@code prefetchInFlight == true}
 * and re-enter the unbounded {@code batchArrived.await()} after the only signal has fired; the
 * later transition to {@code false} carries no further signal, so the receiver hangs forever.
 * This ordering can occur whenever the prefetch produces no message the receiver can consume: an
 * empty successful prefetch or a retryable {@code getNextBatch()} failure (which does not set
 * {@code permanentError}).
 *
 * <p>Each test injects an instrumented {@link ReentrantLock} (via the package-private test-seam
 * constructor) that holds the prefetch worker's first {@code unlock()} until the receiver has
 * re-parked, forcing exactly that interleaving. On the buggy code {@code receive()} never returns
 * and the {@code assertTimeoutPreemptively} trips; on the fixed code (flag cleared under the lock
 * before {@code signalAll}) the receiver re-drives a prefetch, gets the message and returns.
 */
public class AbstractSubscriptionPrefetchDeadlockTest {

  @Test
  @Timeout(30)
  void receiveDoesNotDeadlockWhenPrefetchReturnsEmptyThenMessage() throws Exception {
    InstrumentedLock lock = new InstrumentedLock();
    DeadlockProbeSubscription subscription = new DeadlockProbeSubscription(lock);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(5),
          () -> {
            Message message = subscription.receive();
            assertNotNull(message, "receive() must return the message from the 2nd prefetch");
          },
          "receive() deadlocked: an empty prefetch lost the wakeup and receive() re-parked");
    } finally {
      // Free any prefetch worker still parked in the instrumented unlock() before shutdown, so an
      // abnormal exit above can never leave a non-daemon worker blocking JVM termination.
      lock.release();
      subscription.close();
    }
  }

  @Test
  @Timeout(30)
  void receiveDoesNotDeadlockWhenPrefetchThrowsRetryableThenMessage() throws Exception {
    InstrumentedLock lock = new InstrumentedLock();
    RetryableErrorProbeSubscription subscription = new RetryableErrorProbeSubscription(lock);
    try {
      assertTimeoutPreemptively(
          Duration.ofSeconds(5),
          () -> {
            Message message = subscription.receive();
            assertNotNull(message, "receive() must return the message from the 2nd prefetch");
          },
          "receive() deadlocked: retryable prefetch failure lost the wakeup and re-parked");
    } finally {
      // Free any prefetch worker still parked in the instrumented unlock() before shutdown, so an
      // abnormal exit above can never leave a non-daemon worker blocking JVM termination.
      lock.release();
      subscription.close();
    }
  }

  /**
   * Fake driver whose first {@code doReceiveBatch} returns an EMPTY batch and whose second
   * returns a single delivered message. An empty prefetch is one of the paths that can lose the
   * wakeup, because {@code receive()} checks {@code !queue.isEmpty()} before {@code
   * prefetchInFlight}; the retryable-error path is covered by the sibling test in this class.
   */
  private static final class DeadlockProbeSubscription
      extends AbstractSubscription<DeadlockProbeSubscription> {

    private final AtomicInteger receiveBatchCalls = new AtomicInteger(0);

    DeadlockProbeSubscription(ReentrantLock lock) {
      super("test", "sub", "region", null, lock);
    }

    @Override
    protected List<Message> doReceiveBatch(int batchSize) {
      if (receiveBatchCalls.incrementAndGet() == 1) {
        // Empty prefetch: leaves the queue empty so receive() falls into the prefetchInFlight wait.
        return Collections.emptyList();
      }
      return List.of(Message.builder().withBody("delivered".getBytes()).build());
    }

    @Override
    protected void doSendAcks(List<AckID> ackIDs) {}

    @Override
    protected void doSendNacks(List<AckInfo> nacks) {}

    @Override
    protected Batcher.Options createAckBatcherOptions() {
      return new Batcher.Options()
          .setMaxHandlers(1)
          .setMinBatchSize(1)
          .setMaxBatchSize(1000)
          .setMaxBatchByteSize(0);
    }

    @Override
    public boolean canNack() {
      return false;
    }

    @Override
    public GetAttributeResult getAttributes() {
      return new GetAttributeResult.Builder().name("sub").topic("topic").build();
    }

    @Override
    public boolean isRetryable(Throwable error) {
      return false;
    }

    @Override
    public SubstrateSdkException mapException(Throwable t) {
      return new UnknownException(t);
    }

    @Override
    public Provider.Builder builder() {
      throw new UnsupportedOperationException("builder() is not used by this test");
    }
  }

  /**
   * Fake driver whose first {@code doReceiveBatch} THROWS a retryable error and whose second
   * returns a single delivered message. The catch path in {@code doPrefetch} does not set {@code
   * permanentError} for a retryable error, so pre-fix it clears {@code prefetchInFlight} in the
   * outer {@code finally} without a signal -- the same lost-wakeup window as the empty-batch path.
   */
  private static final class RetryableErrorProbeSubscription
      extends AbstractSubscription<RetryableErrorProbeSubscription> {

    private final AtomicInteger receiveBatchCalls = new AtomicInteger(0);

    RetryableErrorProbeSubscription(ReentrantLock lock) {
      super("test", "sub", "region", null, lock);
    }

    @Override
    protected List<Message> doReceiveBatch(int batchSize) {
      if (receiveBatchCalls.incrementAndGet() == 1) {
        // Retryable failure: no permanentError is set, so receive() falls into the same
        // prefetchInFlight wait it would on an empty batch.
        throw new RetryableProbeException();
      }
      return List.of(Message.builder().withBody("delivered".getBytes()).build());
    }

    @Override
    protected void doSendAcks(List<AckID> ackIDs) {}

    @Override
    protected void doSendNacks(List<AckInfo> nacks) {}

    @Override
    protected Batcher.Options createAckBatcherOptions() {
      return new Batcher.Options()
          .setMaxHandlers(1)
          .setMinBatchSize(1)
          .setMaxBatchSize(1000)
          .setMaxBatchByteSize(0);
    }

    @Override
    public boolean canNack() {
      return false;
    }

    @Override
    public GetAttributeResult getAttributes() {
      return new GetAttributeResult.Builder().name("sub").topic("topic").build();
    }

    @Override
    public boolean isRetryable(Throwable error) {
      return error instanceof RetryableProbeException;
    }

    @Override
    public SubstrateSdkException mapException(Throwable t) {
      return new UnknownException(t);
    }

    @Override
    public Provider.Builder builder() {
      throw new UnsupportedOperationException("builder() is not used by this test");
    }
  }

  /** Marker exception the retryable-error fake classifies as retryable in {@code isRetryable}. */
  private static final class RetryableProbeException extends RuntimeException {}

  /**
   * {@link ReentrantLock} that holds the prefetch worker's FIRST {@code unlock()} (after it has
   * signalled {@code batchArrived} and released the monitor) until the receiver has re-parked. This
   * deterministically opens the window between the worker's {@code unlock()} and the buggy {@code
   * prefetchInFlight.set(false)} in {@code doPrefetch}'s outer {@code finally}.
   */
  private static final class InstrumentedLock extends ReentrantLock {

    // Counted down by the receiver's SECOND await (its re-park); released the held worker unlock.
    private final CountDownLatch receiverReparked = new CountDownLatch(1);
    // Counts the receiver's awaits on batchArrived so we can detect the 2nd (re-park).
    private final AtomicInteger receiverAwaits = new AtomicInteger(0);
    // Ensures only the worker's first unlock is held (later unlocks, e.g. the 2nd prefetch, run
    // normally so the delivered message can wake the receiver on the fixed code).
    private final AtomicBoolean firstPrefetchUnlockHeld = new AtomicBoolean(false);

    @Override
    public void unlock() {
      boolean holdThisUnlock =
          Thread.currentThread().getName().contains("prefetch")
              && firstPrefetchUnlockHeld.compareAndSet(false, true);
      // Release the monitor first so the signalled receiver can re-acquire it and re-check state.
      super.unlock();
      if (holdThisUnlock) {
        // Do not let doPrefetch reach its outer finally (the buggy prefetchInFlight.set(false))
        // until the receiver has re-checked prefetchInFlight and committed to its re-park. Bound
        // the wait so an abnormal receiver exit (assertion failure, interrupt, timeout) can never
        // leave this non-daemon prefetch worker blocked here.
        try {
          receiverReparked.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }

    /** Unconditionally frees a prefetch worker parked in {@link #unlock()} (cleanup safety). */
    void release() {
      receiverReparked.countDown();
    }

    @Override
    public Condition newCondition() {
      return new InstrumentedCondition(super.newCondition(), receiverAwaits, receiverReparked);
    }
  }

  /**
   * Wraps the real {@code batchArrived} condition. Only the receiver awaits it; its second await is
   * the re-park after the first prefetch's signal, at which point we free the held worker unlock.
   */
  private static final class InstrumentedCondition implements Condition {

    private final Condition delegate;
    private final AtomicInteger receiverAwaits;
    private final CountDownLatch receiverReparked;

    InstrumentedCondition(
        Condition delegate, AtomicInteger receiverAwaits, CountDownLatch receiverReparked) {
      this.delegate = delegate;
      this.receiverAwaits = receiverAwaits;
      this.receiverReparked = receiverReparked;
    }

    @Override
    public void await() throws InterruptedException {
      if (receiverAwaits.incrementAndGet() == 2) {
        // The receiver is re-parking after being signalled by the first prefetch: unblock the held
        // worker unlock so its (buggy) prefetchInFlight.set(false) races the re-park. The lock is
        // still held here, so any follow-up prefetch cannot signal until this await enqueues and
        // releases the monitor -- no lost wakeup in the harness itself.
        receiverReparked.countDown();
      }
      delegate.await();
    }

    @Override
    public void awaitUninterruptibly() {
      delegate.awaitUninterruptibly();
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
      return delegate.awaitNanos(nanosTimeout);
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
      return delegate.await(time, unit);
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
      return delegate.awaitUntil(deadline);
    }

    @Override
    public void signal() {
      delegate.signal();
    }

    @Override
    public void signalAll() {
      delegate.signalAll();
    }
  }
}
