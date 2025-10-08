package com.salesforce.multicloudj.pubsub.batcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

/**
 * A batching system that efficiently groups individual items together
 * and processes them in batches.
 * 
 * This batcher can work with any type T. If T implements SizableItem,
 * size-based batching will be used. Otherwise, only count-based batching
 * will be applied.
 *
 * @param <T> the type of items to batch
 */
public class Batcher<T> {

    /**
     * Configuration options that govern how the {@link Batcher} behaves. All limits are inclusive and
     * validated by the {@link Batcher} where relevant.
     *
     * maxHandlers        - Maximum number of concurrent handler threads that may be actively
     *                       processing batches at any given time. Must be &gt;= 1 and determines the
     *                       size of the underlying executor.
     * minBatchSize       - Minimum number of items required before a batch is dispatched during
     *                       normal operation. If fewer than this number are pending, the batcher waits
     *                       for more items unless it is shutting down (in which case remaining items
     *                       are flushed regardless).
     * maxBatchSize       - Hard cap on the number of items allowed in a single batch. A value of 0
     *                       disables this bound. When both maxBatchSize and maxBatchByteSize are set,
     *                       whichever limit is reached first finalises the batch.
     * maxBatchByteSize   - Hard cap on the cumulative byte size of the items in a single batch, as
     *                       reported by {@link SizableItem#getByteSize()}. A value of 0 disables this
     *                       bound.
     */
    @Getter
    @Setter
    @Accessors(chain = true)
    public static class Options {
        /** Maximum number of concurrent handler threads */
        private int maxHandlers = 1;

        /** Minimum number of items required to form a batch */
        private int minBatchSize = 1;

        /** Maximum number of items in a single batch (0 = unlimited) */
        private int maxBatchSize = 0;

        /** Maximum total byte size of a batch (0 = unlimited) */
        private int maxBatchByteSize = 0;
    }

    /**
     * Interface for items that can report their byte size.
     * This is optional - items that don't implement this interface
     * will be treated as having zero byte size.
     */
    public interface SizableItem {
        int getByteSize();
    }
    
    /**
     * Gets the byte size of an item. If the item implements SizableItem,
     * returns its reported size. Otherwise, returns 0.
     * 
     * @param item The item to get size for
     * @return The byte size of the item
     */
    private int getItemByteSize(T item) {
        if (item instanceof SizableItem) {
            return ((SizableItem) item).getByteSize();
        }
        return 0;
    }

    /**
     * Internal class to hold pending items with their completion futures
     */
    private static class Item<T> {
        final T batchItem;
        final CompletableFuture<Void> future;

        Item(T batchItem, CompletableFuture<Void> future) {
            this.batchItem = batchItem;
            this.future = future;
        }
    }

    private final Options options;
    private final Function<List<T>, Void> handler;
    private final ExecutorService executorService;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workCompleted = lock.newCondition();
    private final List<Item<T>> pending = new ArrayList<>();
    private final AtomicInteger activeHandlers = new AtomicInteger(0);
    private volatile boolean shutdown = false;

    /**
     * Creates a new Batcher with default options
     *
     * @param handler function that processes batches of items
     */
    public Batcher(Function<List<T>, Void> handler) {
        this(new Options(), handler);
    }

    /**
     * Creates a new Batcher with custom options
     *
     * @param options configuration options
     * @param handler function that processes batches of items
     */
    public Batcher(Options options, Function<List<T>, Void> handler) {
        if (options.getMaxHandlers() <= 0) {
            throw new InvalidArgumentException("maxHandlers must be greater than 0");
        }

        this.options = options;
        this.handler = handler;

        // Use a fixed-size pool so the underlying thread count never exceeds maxHandlers.
        this.executorService = Executors.newFixedThreadPool(options.getMaxHandlers(), r -> {
            Thread t = new Thread(r, "Batcher-Handler");
            return t;
        });
    }

    /**
     * Adds an item to the batcher and blocks until processing is complete
     *
     * @param item the item to add
     * @throws SubstrateSdkException if the handler throws an exception or if interrupted while waiting
     */
    public void add(T item) {
        try {
            addNoWait(item).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            } else if (e.getCause() != null) {
                throw new SubstrateSdkException("Error executing pubsub batch handler", e.getCause());
            }
            throw new SubstrateSdkException("Error executing pubsub batch handler", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for pubsub batch to complete", e);
        }
    }

    /**
     * Adds an item to the batcher and returns immediately
     *
     * @param item the item to add
     * @return a CompletableFuture that completes when the item is processed
     */
    public CompletableFuture<Void> addNoWait(T item) {
        lock.lock();
        try {
            if (item == null) {
                throw new InvalidArgumentException("Item cannot be null");
            }
            CompletableFuture<Void> future = new CompletableFuture<>();

            if (isShutdown()) {
                future.completeExceptionally(new FailedPreconditionException("Batcher is shut down"));
                return future;
            }

            // Check if item is too large
            int itemSize = getItemByteSize(item);
            if (options.getMaxBatchByteSize() > 0 && itemSize > options.getMaxBatchByteSize()) {
                future.completeExceptionally(new InvalidArgumentException("Item size " + itemSize
                        + " exceeds maximum batch byte size = " + options.getMaxBatchByteSize()));
                return future;
            }

            // Add item to pending list
            pending.add(new Item<>(item, future));

            // Try to start a new batch handler if we have capacity
            tryStartNewBatchHandler(false);

            return future;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Gets the next batch to process, or null if no batch is ready
     * Must be called with lock held
     * @param ignoreMinBatchSize if true, will create a batch even if smaller than minBatchSize
     */
    private List<Item<T>> getNextBatch(boolean ignoreMinBatchSize) {
        if (!ignoreMinBatchSize && pending.size() < options.getMinBatchSize()) {
            return null;
        }

        if (pending.isEmpty()) {
            return null;
        }

        // Optimization: If there's no byte size limit and all pending items can fit into a single
        // batch (i.e., less than maxBatchSize), we can take a fast path and avoid the iterator.
        if (options.getMaxBatchByteSize() == 0 &&
                (options.getMaxBatchSize() == 0 || pending.size() <= options.getMaxBatchSize())) {
            List<Item<T>> batch = new ArrayList<>(pending);
            pending.clear();
            return batch;
        }

        // Build batch respecting size and byte constraints
        List<Item<T>> batch = new ArrayList<>();
        int batchByteSize = 0;

        Iterator<Item<T>> iterator = pending.iterator();
        while (iterator.hasNext()) {
            Item<T> item = iterator.next();

            int itemByteSize = getItemByteSize(item.batchItem);

            boolean reachedMaxSize = options.getMaxBatchSize() > 0 &&
                    batch.size() + 1 > options.getMaxBatchSize();
            boolean reachedMaxByteSize = options.getMaxBatchByteSize() > 0 &&
                    batchByteSize + itemByteSize > options.getMaxBatchByteSize();

            if (reachedMaxSize || reachedMaxByteSize) {
                break;
            }

            batch.add(item);
            batchByteSize += itemByteSize;
            iterator.remove();
        }

        return batch.isEmpty() ? null : batch;
    }

    /**
     * Processes batches in a handler thread
     */
    private void processHandler(List<Item<T>> initialBatch) {
        List<Item<T>> batch = initialBatch;

        try {
            while (batch != null) {
                // Extract items from the batch
                List<T> items = new ArrayList<>();
                for (Item<T> item : batch) {
                    items.add(item.batchItem);
                }

                // Process the batch
                RuntimeException processingError = null;
                try {
                    handler.apply(items);
                } catch (RuntimeException e) {
                    processingError = e;
                }

                // Complete all futures in the batch
                for (Item<T> item : batch) {
                    if (processingError != null) {
                        item.future.completeExceptionally(processingError);
                    } else {
                        item.future.complete(null);
                    }
                }

                // Check for more work
                lock.lock();
                try {
                    batch = getNextBatch(false);
                    if (batch == null) {
                        handlerFinished();
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (RuntimeException e) {
            lock.lock();
            try {
                // When a handler thread fails with an unexpected exception, 
                // fail all pending items and exit.
                for (Item<T> item : pending) {
                    item.future.completeExceptionally(e);
                }
                pending.clear();
                handlerFinished();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * Tries to start a new batch handler if capacity is available and a batch can be formed.
     * Must be called with lock held.
     * @param ignoreMinBatchSize if true, will create a batch even if smaller than minBatchSize
     * @return true if a new batch handler was started, false otherwise
     */
    private boolean tryStartNewBatchHandler(boolean ignoreMinBatchSize) {
        if (activeHandlers.get() < options.getMaxHandlers()) {
            List<Item<T>> batch = getNextBatch(ignoreMinBatchSize);
            if (batch != null) {
                activeHandlers.incrementAndGet();
                executorService.submit(() -> processHandler(batch));
                return true;
            }
        }
        return false;
    }

    /**
     * Decrements active handlers and signals completion. Must be called with lock held.
     */
    private void handlerFinished() {
        activeHandlers.decrementAndGet();
        workCompleted.signalAll();
    }

    /**
     * Shuts down and ensures all pending items are processed
     */
    public void shutdownAndDrain() {
        // Prevent new items
        lock.lock();
        try {
            shutdown = true;
        } finally {
            lock.unlock();
        }

        // Wait for all work to complete using event-driven approach
        lock.lock();
        try {
            // Process remaining items and wait for completion
            while (!pending.isEmpty() || activeHandlers.get() > 0) {
                // Eagerly start handlers for any remaining items
                while (tryStartNewBatchHandler(true)) {
                    Thread.yield();
                }

                // If there's still work in progress, wait for signal
                if (!pending.isEmpty() || activeHandlers.get() > 0) {
                    try {
                        workCompleted.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        // Now shutdown the executor
        executorService.shutdown();
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns true if the batcher is shut down
     */
    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Split determines how to split n (representing n items) into batches based on
     * opts. It returns a slice of batch sizes.
     * 
     * For example, Split(10) might return [10], [5, 5], or [2, 2, 2, 2, 2]
     * depending on opts. opts may be nil to accept defaults.
     * 
     * The sum of returned batches may be less than n (e.g., if n is 10x larger
     * than opts.MaxBatchSize, but opts.MaxHandlers is less than 10).
     * 
     * @param n The total number of items to split
     * @param opts The options to use for splitting
     * @return A list of batch sizes
     */
    public static List<Integer> split(int n, Options opts) {
        if (opts == null) {
            opts = new Options();
        }
        if (n < opts.getMinBatchSize()) {
            // Not enough items to process yet
            return Collections.emptyList();
        }
        if (opts.getMaxBatchSize() == 0) {
            // A single batch can hold everything
            return List.of(n);
        }
        List<Integer> batches = new ArrayList<>();
        while (n >= opts.getMinBatchSize() && batches.size() < opts.getMaxHandlers()) {
            int b = opts.getMaxBatchSize();
            if (b > n) {
                b = n;
            }
            batches.add(b);
            n -= b;
        }
        return batches;
    }
}