package com.salesforce.multicloudj.pubsub.driver;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for subscription implementations.
 * 
 * Implementations should handle proper resource cleanup in the close method,
 * including flushing pending acknowledgments, closing connections, and stopping background threads.
 */
public abstract class AbstractSubscription<T extends AbstractSubscription<T>> implements AutoCloseable {
    
    protected final String providerId;
    protected final String subscriptionName;
    protected final String region;
    protected final URI endpoint;
    protected final URI proxyEndpoint;

    /**
     * Constants class for queue batching and sizing parameters.
     * Contains immutable configuration values used by subscription implementations.
     */
    private static final class QueueConfig {
        
        private QueueConfig() {
        }
        
        /**
         * The desired duration of a subscription's queue of messages (the messages pulled
         * and waiting in memory to be retrieved by Receive callers). This is how long
         * it would take to drain the queue at the current processing rate.
         * The relationship to queue length (number of messages) is:
         *
         *      numMessagesInQueue = desiredQueueDuration / averageProcessTimePerMessage
         *
         * In other words, if it takes 100ms to process a message on average, and we want
         * 2s worth of queued messages, then we need 2/.1 = 20 messages in the queue.
         *
         * If desiredQueueDuration is too small, then there won't be a large enough buffer
         * of messages to handle fluctuations in processing time, and the queue is likely
         * to become empty, reducing throughput. If desiredQueueDuration is too large, then
         * messages will wait in memory for a long time, possibly timing out (that is,
         * their ack deadline will be exceeded). Those messages could have been handled
         * by another process receiving from the same subscription.
         */
        static final Duration DESIRED_QUEUE_DURATION = Duration.ofSeconds(2);

        /**
         * When we have fewer than prefetchRatio * runningBatchSize messages left, that means
         * we expect to run out of messages in expectedReceiveBatchDuration, so we
         * should initiate another ReceiveBatch call.
         * 
         * This is calculated as: expectedReceiveBatchDuration / desiredQueueDuration
         * where expectedReceiveBatchDuration is the expected duration of calls to 
         * driver.ReceiveBatch. We'll try to fetch more messages
         * when the current queue is predicted to be used up in that time.
         */
        static final double PREFETCH_RATIO = 0.5;

        /**
         * The factor by which old batch sizes decay when a new value is added to the
         * running value. The larger this number, the more weight will be given to the
         * newest value in preference to older ones.
         *
         * The delta based on a single value is capped by the growth and shrink factor
         * constants.
         */
        static final double DECAY = 0.5;

        /**
         * The maximum growth factor in a single jump. Higher values mean that the
         * batch size can increase more aggressively. For example, 2.0 means that the
         * batch size will at most double from one ReceiveBatch call to the next.
         */
        static final double MAX_GROWTH_FACTOR = 2.0;

        /**
         * The maximum shrink factor. Lower values mean that the batch size
         * can shrink more aggressively. Note that values less
         * than (1-decay) will have no effect because the running value can't change
         * by more than that.
         */
        static final double MAX_SHRINK_FACTOR = 0.75;

        /**
         * The maximum batch size to request. Setting this too low doesn't allow
         * drivers to get lots of messages at once; setting it too small risks having
         * drivers spend a long time in ReceiveBatch trying to achieve it.
         */
        static final int MAX_BATCH_SIZE = 3000;
    }

    private final ExecutorService backgroundPool;
    private final Batcher.Options receiveBatcherOptions;
    protected final CredentialsOverrider credentialsOverrider;

    /** Synchronization lock for thread-safe access to subscription state and queue operations. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Condition variable signaled when new batches of messages arrive in the queue. */
    private final Condition batchArrived = lock.newCondition();

    /** In-memory queue holding messages that have been fetched but not yet delivered to callers. */
    private final Queue<Message> queue = new ArrayDeque<>();

    /** Set when a fetch, ack, or nack call to the cloud API fails with non-retryable error
     * causing the subscription to stop functioning. */
    protected final AtomicReference<Throwable> permanentError = new AtomicReference<>(null);

    /** Flag indicating whether a prefetch operation is currently in progress. */
    private final AtomicBoolean prefetchInFlight = new AtomicBoolean(false);

    /** Flag indicating whether the subscription has been shut down. */
    protected final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /** Current best estimate for how many messages to fetch in order to maintain the desired queue duration. */
    private int runningBatchSize = 1;

    /** Timestamp (in nanoseconds) when the current throughput measurement window started. */
    private long throughputStart = 0L;

    /** Number of messages processed in the current throughput measurement window. */
    private int throughputCount = 0;

    protected AbstractSubscription(String providerId, String subscriptionName, String region, CredentialsOverrider credentialsOverrider) {
        this.providerId = providerId;
        this.subscriptionName = subscriptionName;
        this.region = region;
        this.endpoint = null;
        this.proxyEndpoint = null;

        this.receiveBatcherOptions = createReceiveBatcherOptions();
        this.credentialsOverrider = credentialsOverrider;
        
        this.ackBatcher = new Batcher<>(createAckBatcherOptions(), this::handleAckBatch);
        this.nackBatcher = new Batcher<>(createNackBatcherOptions(), this::handleNackBatch);

        int poolSize = Math.max(1, this.receiveBatcherOptions.getMaxHandlers());
        this.backgroundPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r);
            t.setName("subscription-" + this.subscriptionName + "-prefetch");
            return t;
        });
    }

    protected AbstractSubscription(Builder<T> builder) {
        this.providerId = builder.providerId;
        this.subscriptionName = builder.subscriptionName;
        this.region = builder.region;
        this.endpoint = builder.endpoint;
        this.proxyEndpoint = builder.proxyEndpoint;

        this.receiveBatcherOptions = createReceiveBatcherOptions();
        this.credentialsOverrider = builder.credentialsOverrider;
        
        this.ackBatcher = new Batcher<>(createAckBatcherOptions(), this::handleAckBatch);
        this.nackBatcher = new Batcher<>(createNackBatcherOptions(), this::handleNackBatch);

        int poolSize = Math.max(1, this.receiveBatcherOptions.getMaxHandlers());
        this.backgroundPool = Executors.newFixedThreadPool(poolSize, r -> {
            Thread t = new Thread(r);
            t.setName("subscription-" + this.subscriptionName + "-prefetch");
            return t;
        });
    }

    public String getProviderId() {
        return providerId;
    }

    /**
     * Receives and returns the next message from the Subscription's queue.
     * 
     * This method provides a simple, blocking interface for consuming messages from a pub/sub subscription.
     * It automatically handles:
     * - Background prefetching to maintain a healthy message queue
     * - Flow control to prevent CPU spinning when no messages are available
     * - Error handling and retry logic for transient failures
     * - Thread-safe concurrent access from multiple threads
     * 
     * <p>When messages are available, this method returns immediately. When the queue is empty,
     * it will wait for new messages to arrive from the cloud provider, with automatic prefetching
     * happening in the background to keep the queue populated.
     * 
     * <p>This method can be called concurrently from multiple threads safely.
     * 
     * @return the next available message from the subscription
     * @throws SubstrateSdkException if the subscription has been shut down, is in a permanent
     *         error state, or if interrupted while waiting for messages
     */
    public Message receive() {
        lock.lock();
        try {
            while (true) {
                // Check if subscription has been shut down
                if (isShutdown.get()) {
                    throw new SubstrateSdkException("Subscription has been shut down");
                }

                // Check for permanent error state
                if (permanentError.get() != null) {
                    unreportedAckErr.set(null);
                    throw new SubstrateSdkException("Subscription in permanent error state", permanentError.get());
                }

                // Check if we need to prefetch
                maybePrefetch();

                // If we have messages in queue, return one
                if (!queue.isEmpty()) {
                    Message m = queue.poll();
                    throughputCount++;
                    return m;
                }

                // No messages available, wait for prefetch to complete
                if (prefetchInFlight.get()) {
                    try {
                        // Wait with timeout to avoid indefinite blocking
                        if (!batchArrived.await(30, TimeUnit.SECONDS)) {
                            throw new SubstrateSdkException("Timeout waiting for messages - prefetch operation took too long");
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SubstrateSdkException("Interrupted while waiting for messages", ie);
                    }
                } else {
                    // No prefetch in flight and no messages available - wait briefly to prevent CPU spinning
                    // This gives time for new messages to arrive or for conditions to change
                    try {
                        if (!batchArrived.await(100, TimeUnit.MILLISECONDS)) {
                            // Timeout is normal - continue loop to check for new messages or prefetch opportunities
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SubstrateSdkException("Interrupted while waiting for messages", ie);
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Provider specific implementation must fetch at most batchSize messages from the remote service.
     */
    protected abstract List<Message> doReceiveBatch(int batchSize);

    /**
     * Creates batcher options for this subscription. Provides defaults that
     * cloud providers can override to provide provider-specific batching configuration
     * that aligns with their service limits and performance characteristics.
     */
    protected Batcher.Options createReceiveBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(1)
            .setMinBatchSize(1)
            .setMaxBatchSize(3000)
            .setMaxBatchByteSize(0); // No limit
    }

    private final Batcher<AckID> ackBatcher;
    private final Batcher<AckID> nackBatcher;
    protected final AtomicReference<Throwable> unreportedAckErr = new AtomicReference<>(); // Unreported error from background SendAcks
    
    /**
     * Sends acknowledgment for a single message.
     * 
     * <p>This method enqueues the acknowledgment for batch processing and returns immediately.
     * The actual acknowledgment is sent asynchronously in batches for efficiency.
     * 
     * @param ackID the acknowledgment identifier
     */
    public void sendAck(AckID ackID) {
        if (ackID == null) {
            RuntimeException error = new InvalidArgumentException("AckID cannot be null");
            permanentError.set(error);
            throw error;
        }
        
        validateAckIDType(ackID);
        
        ackBatcher.addNoWait(ackID);
    }

    /**
     * Sends acknowledgments for multiple messages.
     * 
     * <p>The returned Future completes immediately upon enqueueing the acknowledgments
     * for batch processing, without waiting for the underlying RPC to complete.
     * 
     * @param ackIDs the list of acknowledgment identifiers
     * @return a CompletableFuture that completes when acknowledgments are enqueued
     */
    public CompletableFuture<Void> sendAcks(List<AckID> ackIDs) {
        if (ackIDs == null || ackIDs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        
        for (AckID ackID : ackIDs) {
            if (ackID == null) {
                RuntimeException error = new InvalidArgumentException("AckID cannot be null in batch acknowledgment");
                permanentError.set(error);
                throw error;
            }
            validateAckIDType(ackID);
        }
        
        for (AckID ackID : ackIDs) {
            ackBatcher.addNoWait(ackID);
        }
        
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Sends negative acknowledgment for a single message.
     * 
     * @param ackID the acknowledgment ID to negatively acknowledge
     * @throws InvalidArgumentException if ackID is null
     * @throws SubstrateSdkException if the subscription is in an error state or has been shut down
     */
    public void sendNack(AckID ackID) {
        if (isShutdown.get()) {
            throw new SubstrateSdkException("Subscription has been shut down");
        }
        
        if (ackID == null) {
            RuntimeException error = new InvalidArgumentException("AckID cannot be null");
            permanentError.set(error);
            throw error;
        }
        
        validateAckIDType(ackID);
        nackBatcher.addNoWait(ackID);
    }
    
    /**
     * Sends negative acknowledgment for multiple messages.
     * 
     * @param ackIDs the list of acknowledgment IDs to negatively acknowledge
     * @return a CompletableFuture that completes when the nack is queued
     * @throws InvalidArgumentException if ackIDs is null or contains null elements
     * @throws SubstrateSdkException if the subscription is in an error state or has been shut down
     */
    public CompletableFuture<Void> sendNacks(List<AckID> ackIDs) {
        if (isShutdown.get()) {
            throw new SubstrateSdkException("Subscription has been shut down");
        }
        
        if (ackIDs == null) {
            RuntimeException error = new InvalidArgumentException("AckIDs list cannot be null");
            permanentError.set(error);
            throw error;
        }
        
        if (ackIDs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        
        for (AckID ackID : ackIDs) {
            if (ackID == null) {
                RuntimeException error = new InvalidArgumentException("AckID cannot be null in batch negative acknowledgment");
                permanentError.set(error);
                throw error;
            }
            validateAckIDType(ackID);
        }
        
        for (AckID ackID : ackIDs) {
            nackBatcher.addNoWait(ackID);
        }
        
        return CompletableFuture.completedFuture(null);
    }
    
    public abstract boolean canNack();
    public abstract boolean isRetryable(Throwable error);
    public abstract Map<String, String> getAttributes();
    
    protected abstract void doSendAcks(List<AckID> ackIDs);
    protected abstract void doSendNacks(List<AckID> ackIDs);
    protected abstract String getMessageId(AckID ackID);
    protected void validateAckIDType(AckID ackID) {}
    
    /**
     * Handles a batch of acknowledgments.
     * This is called by the ackBatcher for efficient batch processing.
     */
    private Void handleAckBatch(List<AckID> ackIDs) {
        if (ackIDs.isEmpty()) {
            return null;
        }

        try {
            doSendAcks(ackIDs);
            
        } catch (Exception e) {
            if (!isRetryable(e)) {
                permanentError.set(e);
            }
            unreportedAckErr.set(e);
            throw new SubstrateSdkException("Batch acknowledge failed", e);
        }
        
        return null;
    }
    
    /**
     * Handles a batch of negative acknowledgments.
     * This is called by the nackBatcher for efficient batch processing.
     */
    private Void handleNackBatch(List<AckID> ackIDs) {
        if (ackIDs.isEmpty()) {
            return null;
        }

        try {
            doSendNacks(ackIDs);
            
        } catch (Exception e) {
            if (!isRetryable(e)) {
                permanentError.set(e);
            }
            unreportedAckErr.set(e);
            throw new SubstrateSdkException("Batch negative acknowledge failed", e);
        }
        
        return null;
    }
    
    /**
     * Creates batcher options for acknowledgment operations.
     */
    protected Batcher.Options createAckBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(2)
            .setMinBatchSize(1)
            .setMaxBatchSize(1000)
            .setMaxBatchByteSize(0);
    }
    
    protected Batcher.Options createNackBatcherOptions() {
        return new Batcher.Options()
            .setMaxHandlers(2)
            .setMinBatchSize(1)
            .setMaxBatchSize(1000)
            .setMaxBatchByteSize(0);
    }
    
    
    /**
     * Stores an unreported ack error from background SendAcks.
     */
    protected void storeAckError(Throwable error) {
        unreportedAckErr.set(error);
    }
    
    /**
     * Gets the permanent error and clears it.
     */
    public Throwable getLastError() {
        return permanentError.getAndSet(null);
    }
    
    /**
     * Gets the unreported ack error and clears it.
     */
    public Throwable getLastAckError() {
        return unreportedAckErr.getAndSet(null);
    }

    private void maybePrefetch() {
        // Don't start new prefetch operations if shutting down or a previous prefetch operation is ongoing
        if (isShutdown.get() || prefetchInFlight.get()) {
            return;
        }
        int qSize = queue.size();
        if (qSize <= (int) (runningBatchSize * QueueConfig.PREFETCH_RATIO)) {
            int batchSize = updateBatchSize();
            if (prefetchInFlight.compareAndSet(false, true)) {
                backgroundPool.submit(() -> doPrefetch(batchSize));
            }
        }
    }

    /**
     * Dynamically calculates and updates the optimal batch size for message fetching. Steps:
     * 1. Measures current message processing throughput (messages per second)
     * 2. Calculates ideal batch size needed to maintain QueueConfig.DESIRED_QUEUE_DURATION
     * 3. Gradually moves current batch size toward the ideal using exponential decay
     * 4. Applies growth and shrink limits to prevent dramatic size changes
     * 
     * @return The calculated batch size for the next prefetch operation, 
     *         capped by receiveBatcherOptions.getMaxBatchSize()
     */
    private int updateBatchSize() {
        if (receiveBatcherOptions.getMaxHandlers() == 1 && receiveBatcherOptions.getMaxBatchSize() == 1) {
            return 1;
        }

        long now = System.nanoTime();
        if (throughputStart != 0L) {
            // Update runningBatchSize based on throughput since our last time here,
            // as measured by the ratio of the number of messages returned to elapsed time.
            long elapsedNanos = now - throughputStart;
            long minElapsedNanos = Duration.ofMillis(100).toNanos();
            if (elapsedNanos < minElapsedNanos) {
                // Avoid divide-by-zero and huge numbers.
                elapsedNanos = minElapsedNanos;
            }

            double elapsedSeconds = elapsedNanos / 1e9;
            double msgsPerSec = throughputCount / elapsedSeconds;

            // The "ideal" batch size is how many messages we'd need in the queue to
            // support desiredQueueDuration at the msgsPerSec rate.
            double idealBatchSize = QueueConfig.DESIRED_QUEUE_DURATION.getSeconds() * msgsPerSec;

            // Move runningBatchSize towards the ideal.
            // We first combine the previous value and the new value, with weighting
            // based on decay, and then cap the growth/shrinkage.
            double newBatchSize = runningBatchSize * (1 - QueueConfig.DECAY) + idealBatchSize * QueueConfig.DECAY;

            double maxSize = runningBatchSize * QueueConfig.MAX_GROWTH_FACTOR;
            double minSize = runningBatchSize * QueueConfig.MAX_SHRINK_FACTOR;

            if (newBatchSize > maxSize) {
                runningBatchSize = (int) maxSize;
            } else if (newBatchSize < minSize) {
                runningBatchSize = (int) minSize;
            } else {
                runningBatchSize = (int) newBatchSize;
            }
        }

        throughputStart = now;
        throughputCount = 0;

        return Math.min(runningBatchSize, QueueConfig.MAX_BATCH_SIZE);
    }

    /**
     * Fetches the next batch of messages from the subscription, handling large requests efficiently.
     * 
     * This method automatically splits large batch requests into multiple smaller concurrent requests 
     * when necessary to comply with cloud provider limits:
     * 
     * 1. Uses Batcher.split() to divide the request based on receiveBatcherOptions limits
     * 2. For single batch: Makes direct synchronous call to doReceiveBatch()
     * 3. For multiple batches: Creates concurrent CompletableFuture tasks for parallel execution
     * 4. Combines all results into a single message list for the caller
     * 
     * @param nMessages The total number of messages requested by caller
     * @return Combined list of messages from all cloud API calls
     * @throws SubstrateSdkException If any batch operation fails or is interrupted
     */
    private List<Message> getNextBatch(int nMessages) {
        List<Integer> batchSizes = Batcher.split(nMessages, receiveBatcherOptions);
        if (batchSizes.isEmpty()) {
            return Collections.emptyList();
        }
        if (batchSizes.size() == 1) {
            return doReceiveBatch(batchSizes.get(0));
        }
        List<CompletableFuture<List<Message>>> futures = new ArrayList<>();
        for (int size : batchSizes) {
            futures.add(CompletableFuture.supplyAsync(() -> doReceiveBatch(size), backgroundPool));
        }
        List<Message> combined = new ArrayList<>();
        for (CompletableFuture<List<Message>> f : futures) {
            try {
                combined.addAll(f.get());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new SubstrateSdkException("Interrupted while waiting for batch futures", ie);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else if (cause != null) {
                    throw new SubstrateSdkException("Error receiving batch subscription", cause);
                }
                throw new SubstrateSdkException("Error receiving batch subscription", ee);
            }
        }
        return combined;
    }

    private void doPrefetch(int batchSize) {
        try {
            List<Message> msgs = getNextBatch(batchSize);
            lock.lock();
            try {
                queue.addAll(msgs);
                batchArrived.signalAll();
            } finally {
                lock.unlock();
            }
        } catch (Throwable t) {
            lock.lock();
            try {
                // Set permanentError if the error is not retryable
                if (!isRetryable(t)) {
                    permanentError.set(t);
                }
                batchArrived.signalAll();
            } finally {
                lock.unlock();
            }
        } finally {
            prefetchInFlight.set(false);
        }
    }

    /**
     * Implements AutoCloseable interface for try-with-resources support.
     *
     * Shuts down the subscription and releases all resources.
     * 
     * This method should:
     *  - Flush any pending acknowledgments or nacks
     *  - Close network connections
     *  - Release any other provider-specific resources
     *
     * After calling this method, the subscription should not accept new operations.
     * It is safe to call this method multiple times.
     */
    @Override
    public void close() throws Exception {
        // Set shutdown flag first
        isShutdown.set(true);

        // Wake up any threads waiting in receive() method BEFORE shutting down background pool
        lock.lock();
        try {
            batchArrived.signalAll();
        } finally {
            lock.unlock();
        }

        if (ackBatcher != null) {
            ackBatcher.shutdownAndDrain();
        }
        
        if (nackBatcher != null) {
            nackBatcher.shutdownAndDrain();
        }
        
        // Now shutdown background pool to stop all ongoing prefetch operations
        if (backgroundPool != null && !backgroundPool.isShutdown()) {
            backgroundPool.shutdown();
            try {
                backgroundPool.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        
        Throwable ackError = unreportedAckErr.getAndSet(null);
        if (ackError != null) {
            throw new SubstrateSdkException("Unreported ack error during shutdown", ackError);
        }
    }

    public abstract Class<? extends SubstrateSdkException> getException(Throwable t);

    public abstract static class Builder<T extends AbstractSubscription<T>> {
        protected String providerId;
        protected String subscriptionName;
        protected String region;
        protected URI endpoint;
        protected URI proxyEndpoint;
        protected CredentialsOverrider credentialsOverrider;

        public Builder<T> withSubscriptionName(String subscriptionName) {
            this.subscriptionName = subscriptionName;
            return this;
        }

        public Builder<T> withRegion(String region) {
            this.region = region;
            return this;
        }

        public Builder<T> withEndpoint(URI endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder<T> withProxyEndpoint(URI proxyEndpoint) {
            this.proxyEndpoint = proxyEndpoint;
            return this;
        }

        public Builder<T> withCredentialsOverrider(CredentialsOverrider credentialsOverrider) {
            this.credentialsOverrider = credentialsOverrider;
            return this;
        }

        public abstract T build();
    }
}