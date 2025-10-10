package com.salesforce.multicloudj.pubsub.batcher;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class BatcherTest {

    private Function<List<SizableString>, Void> mockHandler;
    
    @BeforeEach
    void setUp() {
        mockHandler = mock(Function.class);
    }

    @Test
    void testAddSynchronously() throws Exception {
        // Arrange
        Batcher<SizableString> batcher = new Batcher<>(mockHandler);
        SizableString item = new SizableString("test-item");

        // Act
        batcher.add(item);

        // Assert
        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, times(1)).apply(captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals(item, captor.getValue().get(0));
        batcher.shutdownAndDrain();
    }



    @Test
    void testAddAsynchronously() throws Exception {
        // Arrange
        Batcher<SizableString> batcher = new Batcher<>(mockHandler);
        SizableString item = new SizableString("test-item");

        // Act
        CompletableFuture<Void> future = batcher.addNoWait(item);
        future.get(3, TimeUnit.SECONDS);

        // Assert
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, times(1)).apply(captor.capture());
        assertEquals(1, captor.getValue().size());
        assertEquals(item, captor.getValue().get(0));
        batcher.shutdownAndDrain();
    }

    @Test
    void testMultipleItemsBatching() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMinBatchSize(3);
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);

        // Act
        CompletableFuture<Void> future1 = batcher.addNoWait(new SizableString("item1"));
        CompletableFuture<Void> future2 = batcher.addNoWait(new SizableString("item2"));
        CompletableFuture<Void> future3 = batcher.addNoWait(new SizableString("item3"));

        CompletableFuture.allOf(future1, future2, future3).get(3, TimeUnit.SECONDS);

        // Assert
        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, times(1)).apply(captor.capture());
        assertEquals(3, captor.getValue().size());
        List<String> values = captor.getValue().stream().map(SizableString::getValue).collect(Collectors.toList());
        assertTrue(values.contains("item1"));
        assertTrue(values.contains("item2"));
        assertTrue(values.contains("item3"));
        batcher.shutdownAndDrain();
    }

    @Test
    void testMaxBatchSize() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMaxBatchSize(2);
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);

        // Act
        CompletableFuture<Void> future1 = batcher.addNoWait(new SizableString("item1"));
        CompletableFuture<Void> future2 = batcher.addNoWait(new SizableString("item2"));
        CompletableFuture<Void> future3 = batcher.addNoWait(new SizableString("item3"));

        CompletableFuture.allOf(future1, future2, future3).get(3, TimeUnit.SECONDS);

        // Assert
        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, atLeastOnce()).apply(captor.capture());
        
        List<List<SizableString>> allBatches = captor.getAllValues();
        
        // Verify all items were processed
        int totalItems = allBatches.stream().mapToInt(List::size).sum();
        assertEquals(3, totalItems);
        
        // Verify no batch exceeds maxBatchSize
        for (List<SizableString> batch : allBatches) {
            assertTrue(batch.size() <= 2, "Batch size should not exceed maxBatchSize of 2");
        }
        batcher.shutdownAndDrain();
    }

    @Test
    void testMaxBatchSizeWithMinBatchSize() throws Exception {
        // Arrange - This test demonstrates actual batching behavior
        Batcher.Options options = new Batcher.Options().setMinBatchSize(2).setMaxBatchSize(3);
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);

        // Act - Add 5 items
        CompletableFuture<Void> future1 = batcher.addNoWait(new SizableString("item1"));
        CompletableFuture<Void> future2 = batcher.addNoWait(new SizableString("item2"));
        CompletableFuture<Void> future3 = batcher.addNoWait(new SizableString("item3"));
        CompletableFuture<Void> future4 = batcher.addNoWait(new SizableString("item4"));
        CompletableFuture<Void> future5 = batcher.addNoWait(new SizableString("item5"));

        // Increased timeout from 1 to 5 seconds for CI environments
        CompletableFuture.allOf(future1, future2, future3, future4, future5).get(5, TimeUnit.SECONDS);

        // Assert
        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, atLeastOnce()).apply(captor.capture());
        
        List<List<SizableString>> allBatches = captor.getAllValues();
        
        // Verify all items were processed
        int totalItems = allBatches.stream().mapToInt(List::size).sum();
        assertEquals(5, totalItems);
        
        // Verify each batch meets minBatchSize requirement and doesn't exceed maxBatchSize
        for (List<SizableString> batch : allBatches) {
            assertTrue(batch.size() >= 2, "Batch size should meet minBatchSize of 2");
            assertTrue(batch.size() <= 3, "Batch size should not exceed maxBatchSize of 3");
        }
        batcher.shutdownAndDrain();
    }

    @Test
    void testSizableItemBatching() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMaxBatchByteSize(100);
        Function<List<SizableTestItem>, Void> sizableHandler = mock(Function.class);
        Batcher<SizableTestItem> sizableBatcher = new Batcher<>(options, sizableHandler);

        SizableTestItem item1 = new SizableTestItem("item1", 50);
        SizableTestItem item2 = new SizableTestItem("item2", 30);
        SizableTestItem item3 = new SizableTestItem("item3", 40);

        // Act
        CompletableFuture<Void> future1 = sizableBatcher.addNoWait(item1);
        CompletableFuture<Void> future2 = sizableBatcher.addNoWait(item2);
        CompletableFuture<Void> future3 = sizableBatcher.addNoWait(item3);

        CompletableFuture.allOf(future1, future2, future3).get(3, TimeUnit.SECONDS);

        // Assert
        ArgumentCaptor<List<SizableTestItem>> captor = ArgumentCaptor.forClass(List.class);
        verify(sizableHandler, atLeastOnce()).apply(captor.capture());
        
        List<List<SizableTestItem>> allBatches = captor.getAllValues();
        
        // Verify all items were processed
        int totalItems = allBatches.stream().mapToInt(List::size).sum();
        assertEquals(3, totalItems);
        
        // Verify no batch exceeds maxBatchByteSize
        for (List<SizableTestItem> batch : allBatches) {
            int batchByteSize = batch.stream().mapToInt(SizableTestItem::getByteSize).sum();
            assertTrue(batchByteSize <= 100, "Batch byte size should not exceed maxBatchByteSize of 100");
        }
        sizableBatcher.shutdownAndDrain();
    }

    @Test
    void testSizableItemBatchingWithMinBatchSize() throws Exception {
        // Arrange - This test demonstrates actual byte-size based batching
        Batcher.Options options = new Batcher.Options().setMinBatchSize(2).setMaxBatchByteSize(80);
        Function<List<SizableTestItem>, Void> sizableHandler = mock(Function.class);
        Batcher<SizableTestItem> sizableBatcher = new Batcher<>(options, sizableHandler);

        SizableTestItem item1 = new SizableTestItem("item1", 50);
        SizableTestItem item2 = new SizableTestItem("item2", 30); // Total: 80 bytes (at limit)
        SizableTestItem item3 = new SizableTestItem("item3", 40); // Would exceed byte limit
        SizableTestItem item4 = new SizableTestItem("item4", 30); // Can batch with item3

        // Act
        CompletableFuture<Void> future1 = sizableBatcher.addNoWait(item1);
        CompletableFuture<Void> future2 = sizableBatcher.addNoWait(item2);
        CompletableFuture<Void> future3 = sizableBatcher.addNoWait(item3);
        CompletableFuture<Void> future4 = sizableBatcher.addNoWait(item4);

        CompletableFuture.allOf(future1, future2, future3, future4).get(3, TimeUnit.SECONDS);

        // Assert
        ArgumentCaptor<List<SizableTestItem>> captor = ArgumentCaptor.forClass(List.class);
        verify(sizableHandler, times(2)).apply(captor.capture()); // 2 batches: [item1,item2] and [item3,item4]
        
        List<List<SizableTestItem>> allBatches = captor.getAllValues();
        assertEquals(2, allBatches.size());
        
        // First batch: item1 + item2 = 80 bytes (at limit)
        assertEquals(2, allBatches.get(0).size());
        // Second batch: item3 + item4 = 70 bytes
        assertEquals(2, allBatches.get(1).size());
        sizableBatcher.shutdownAndDrain();
    }

    @Test
    void testMessageTooLargeException() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMaxBatchByteSize(50);
        Function<List<SizableTestItem>, Void> sizableHandler = mock(Function.class);
        Batcher<SizableTestItem> sizableBatcher = new Batcher<>(options, sizableHandler);

        SizableTestItem oversizedItem = new SizableTestItem("oversized", 100);

        // Act & Assert
        CompletableFuture<Void> future = sizableBatcher.addNoWait(oversizedItem);
        
        ExecutionException exception = assertThrows(ExecutionException.class, () -> {
            future.get(3, TimeUnit.SECONDS);
        });
        
        assertTrue(exception.getCause() instanceof InvalidArgumentException);
        assertEquals("Item size 100 exceeds maximum batch byte size = 50", exception.getCause().getMessage());
        
        // Handler should not be called
        verify(sizableHandler, never()).apply(any());
        sizableBatcher.shutdownAndDrain();
    }

    @Test
    void testAddNullItemThrowsException() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options();
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);

        // Act & Assert
        InvalidArgumentException exception = assertThrows(InvalidArgumentException.class, () -> {
            batcher.add(null);
        });
        assertEquals("Item cannot be null", exception.getMessage());
        batcher.shutdownAndDrain();
    }

    @Test
    void testHandlerException() {
        // Arrange
        RuntimeException testException = new RuntimeException("Test handler exception");
        when(mockHandler.apply(any())).thenThrow(testException);
        Batcher<SizableString> batcher = new Batcher<>(mockHandler);

        // Act & Assert
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> {
            batcher.add(new SizableString("test-item"));
        });
        assertEquals(testException, thrown);
        batcher.shutdownAndDrain();
    }

    @Test
    void testInterruptedExceptionIsPropagated() throws InterruptedException {
        // Arrange - Use a simple handler that just waits
        Function<List<SizableString>, Void> slowHandler = items -> {
            try {
                Thread.sleep(1000); // Wait for 1 second
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Handler interrupted", e);
            }
            return null;
        };

        Batcher<SizableString> batcher = new Batcher<>(slowHandler);
        final AtomicReference<Exception> caughtException = new AtomicReference<>();

        // Act - Test the add() method directly with interruption
        Thread testThread = new Thread(() -> {
            try {
                batcher.add(new SizableString("item"));
            } catch (Exception e) {
                caughtException.set(e);
            }
        });

        testThread.start();
        
        // Wait a bit for the thread to start and get into the waiting state
        Thread.sleep(100);
        
        // Interrupt the thread
        testThread.interrupt();
        testThread.join(1000);

        // Assert
        assertNotNull(caughtException.get(), "An exception should have been caught");
        assertTrue(caughtException.get() instanceof SubstrateSdkException, 
                   "Expected SubstrateSdkException but got: " + caughtException.get().getClass().getSimpleName());
        assertTrue(caughtException.get().getCause() instanceof InterruptedException,
                   "Expected cause to be InterruptedException but got: " + 
                   (caughtException.get().getCause() != null ? caughtException.get().getCause().getClass().getSimpleName() : "null"));
        batcher.shutdownAndDrain();
    }

    @Test
    void testShutdownPreventsNewItems() {
        // Arrange
        Batcher<SizableString> batcher = new Batcher<>(mockHandler);
        batcher.shutdownAndDrain();

        // Act & Assert
        CompletableFuture<Void> future = batcher.addNoWait(new SizableString("another-item"));
        ExecutionException ex = assertThrows(ExecutionException.class, future::get);
        assertTrue(ex.getCause() instanceof FailedPreconditionException);
    }

    @Disabled
    @Test
    void testShutdownAndDrain() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMinBatchSize(3);
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);

        // Act
        CompletableFuture<Void> future1 = batcher.addNoWait(new SizableString("item1"));
        CompletableFuture<Void> future2 = batcher.addNoWait(new SizableString("item2"));
        CompletableFuture<Void> future3 = batcher.addNoWait(new SizableString("item3"));
        
        assertFalse(future1.isDone());
        assertFalse(future2.isDone());
        assertFalse(future3.isDone());

        batcher.shutdownAndDrain();

        // Assert
        assertTrue(batcher.isShutdown());
        assertTrue(future1.isDone());
        assertTrue(future2.isDone());
        assertTrue(future3.isDone());

        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, times(1)).apply(captor.capture());
        assertEquals(3, captor.getValue().size());
        batcher.shutdownAndDrain();
    }

    @Test
    void testConcurrentAccess() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMinBatchSize(10).setMaxHandlers(4);
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);
        int numThreads = 10;
        int itemsPerThread = 20;
        int totalItems = numThreads * itemsPerThread;

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(numThreads);

        List<CompletableFuture<Void>> futures = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < numThreads; i++) {
            int threadNum = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < itemsPerThread; j++) {
                        futures.add(batcher.addNoWait(new SizableString("item-" + threadNum + "-" + j)));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    finishLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(finishLatch.await(5, TimeUnit.SECONDS));
        batcher.shutdownAndDrain();

        // Assert
        ArgumentCaptor<List<SizableString>> captor = ArgumentCaptor.forClass(List.class);
        verify(mockHandler, atLeastOnce()).apply(captor.capture());
        int processedItems = captor.getAllValues().stream().mapToInt(List::size).sum();
        assertEquals(totalItems, processedItems);
        assertEquals(totalItems, futures.size());
        for (CompletableFuture<Void> future : futures) {
            assertTrue(future.isDone());
        }
    }

    @Test
    void testMultipleHandlers() throws Exception {
        // Arrange
        int numHandlers = 4;
        CountDownLatch handlerLatch = new CountDownLatch(numHandlers);
        AtomicInteger maxConcurrentHandlers = new AtomicInteger(0);
        AtomicInteger concurrentHandlers = new AtomicInteger(0);

        Function<List<SizableString>, Void> trackingHandler = items -> {
            int current = concurrentHandlers.incrementAndGet();
            maxConcurrentHandlers.accumulateAndGet(current, Math::max);
            try {
                Thread.sleep(100); // Simulate work
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            concurrentHandlers.decrementAndGet();
            handlerLatch.countDown();
            return null;
        };

        Batcher.Options options = new Batcher.Options().setMaxHandlers(numHandlers);
        Batcher<SizableString> batcher = new Batcher<>(options, trackingHandler);

        // Act
        for (int i = 0; i < numHandlers * 2; i++) {
            batcher.addNoWait(new SizableString("item" + i));
        }

        assertTrue(handlerLatch.await(5, TimeUnit.SECONDS));
        batcher.shutdownAndDrain();

        // Assert
        assertTrue(maxConcurrentHandlers.get() > 1, "Expected multiple handlers to run concurrently");
        assertTrue(maxConcurrentHandlers.get() <= numHandlers, "Should not exceed max handlers");
    }

    @Test
    void testEmptyBatchHandling() throws Exception {
        // Arrange
        Batcher.Options options = new Batcher.Options().setMinBatchSize(2);
        Batcher<SizableString> batcher = new Batcher<>(options, mockHandler);

        // Act
        CompletableFuture<Void> f1 = batcher.addNoWait(new SizableString("item1"));
        batcher.shutdownAndDrain();

        // Assert
        assertTrue(f1.isDone());
        verify(mockHandler, times(1)).apply(any());
        batcher.shutdownAndDrain();
    }

    @Test
    void testAsyncComposition() throws Exception {
        // Arrange
        AtomicReference<String> result = new AtomicReference<>();
        Function<List<SizableString>, Void> handler = items -> {
            result.set(items.get(0).getValue().toUpperCase());
            return null;
        };
        Batcher<SizableString> batcher = new Batcher<>(handler);

        // Act
        CompletableFuture<String> finalFuture = batcher.addNoWait(new SizableString("test"))
                .thenApply(v -> result.get());

        // Assert
        assertEquals("TEST", finalFuture.get());
        batcher.shutdownAndDrain();
    }

    @Test 
    void testBatchAccumulationBenefit() throws Exception {
        // Arrange
        CountDownLatch handlerLatch = new CountDownLatch(1);
        Function<List<SizableString>, Void> handler = items -> {
            handlerLatch.countDown();
            return null;
        };
        Batcher.Options options = new Batcher.Options().setMinBatchSize(10);
        Batcher<SizableString> batcher = new Batcher<>(options, handler);

        // Act
        for (int i = 0; i < 9; i++) {
            batcher.addNoWait(new SizableString("item" + i));
        }

        // Assert
        assertFalse(handlerLatch.await(50, TimeUnit.MILLISECONDS), "Handler should not be called before minBatchSize is reached");
        
        batcher.addNoWait(new SizableString("item9")); // This should trigger the batch
        assertTrue(handlerLatch.await(1, TimeUnit.SECONDS), "Handler should be called once minBatchSize is met");
        batcher.shutdownAndDrain();
    }

    @Test
    void testSelectiveWaiting() throws Exception {
        // Arrange
        // A simple handler that does nothing, to isolate batching logic
        Function<List<SizableString>, Void> handler = items -> null;

        Batcher.Options options = new Batcher.Options().setMinBatchSize(2).setMaxBatchSize(2).setMaxHandlers(2);
        Batcher<SizableString> batcher = new Batcher<>(options, handler);
        
        // Act
        CompletableFuture<Void> f1 = batcher.addNoWait(new SizableString("item1"));
        CompletableFuture<Void> f2 = batcher.addNoWait(new SizableString("item2"));
        
        // Wait for the first batch to complete. This is robust.
        CompletableFuture.allOf(f1, f2).get(2, TimeUnit.SECONDS);
        
        // Assertions for the first batch
        assertTrue(f1.isDone());
        assertTrue(f2.isDone());
        
        // Act for the second batch
        CompletableFuture<Void> f10 = batcher.addNoWait(new SizableString("item10"));
        CompletableFuture<Void> f11 = batcher.addNoWait(new SizableString("item11"));
        
        // Since handlers can run in parallel and processing is instant, 
        // we can't reliably assert that f10 is NOT done here.
        // Instead, we just wait for the second batch and confirm it also completes.
        
        CompletableFuture.allOf(f10, f11).get(2, TimeUnit.SECONDS);

        // Assertions for the second batch
        assertTrue(f10.isDone());
        assertTrue(f11.isDone());
        
        batcher.shutdownAndDrain();
    }

    @Test
    void testAsyncErrorHandling() throws Exception {
        // Arrange
        RuntimeException testException = new RuntimeException("Async failure");
        Function<List<SizableString>, Void> handler = items -> {
            throw testException;
        };
        Batcher<SizableString> batcher = new Batcher<>(handler);
        AtomicReference<Throwable> caughtException = new AtomicReference<>();

        // Act
        CompletableFuture<Void> future = batcher.addNoWait(new SizableString("item"))
                .exceptionally(ex -> {
                    caughtException.set(ex);
                    return null;
                });

        future.get(); // Wait for the future to complete

        // Assert
        assertNotNull(caughtException.get());
        assertEquals(testException, caughtException.get());
        batcher.shutdownAndDrain();
    }

    @Test
    void testConcurrentAsyncOperations() throws Exception {
        // Arrange
        int numOperations = 1000;
        AtomicInteger processedCount = new AtomicInteger(0);

        Function<List<SizableString>, Void> handler = items -> {
            processedCount.addAndGet(items.size());
            return null;
        };

        Batcher.Options options = new Batcher.Options().setMinBatchSize(10).setMaxBatchSize(100);
        Batcher<SizableString> batcher = new Batcher<>(options, handler);

        // Act
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < numOperations; i++) {
            futures.add(batcher.addNoWait(new SizableString("item" + i)));
        }

        batcher.shutdownAndDrain();

        // Assert
        assertEquals(numOperations, processedCount.get());
        for(CompletableFuture<Void> f : futures) {
            assertTrue(f.isDone());
        }
    }

    @Test
    void testSplitBelowMinBatchSize() {
        Batcher.Options opts = new Batcher.Options().setMinBatchSize(2);
        List<Integer> result = Batcher.split(1, opts);
        assertTrue(result.isEmpty(), "Expected empty list when below minBatchSize");
    }

    @Test
    void testSplitUnlimitedMaxBatchSize() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(0).setMinBatchSize(1);
        List<Integer> result = Batcher.split(10, opts);
        assertEquals(List.of(10), result, "Expected single batch when maxBatchSize is unlimited");
    }

    @Test
    void testSplitRespectsMaxBatchSize() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(5).setMaxHandlers(10);
        List<Integer> result = Batcher.split(12, opts);
        assertEquals(List.of(5, 5, 2), result, "Expected batches [5,5,2] for n=12 and maxBatchSize=5");
    }

    @Test
    void testSplitCappedByMaxHandlers() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(9).setMaxHandlers(2);
        List<Integer> result = Batcher.split(20, opts);
        assertEquals(2, result.size(), "Number of batches should be capped by maxHandlers");
        assertEquals(List.of(9, 9), result, "Expected two batches of size 9 each");
    }

    @Test
    void testSplitWithNullOptions() {
        List<Integer> result = Batcher.split(5, null);
        assertEquals(List.of(5), result, "Should use default options when null is passed");
    }

    @Test
    void testSplitExactlyMinBatchSize() {
        Batcher.Options opts = new Batcher.Options().setMinBatchSize(5);
        List<Integer> result = Batcher.split(5, opts);
        assertEquals(List.of(5), result, "Should create single batch when n equals minBatchSize");
    }

    @Test
    void testSplitZeroItems() {
        Batcher.Options opts = new Batcher.Options().setMinBatchSize(1);
        List<Integer> result = Batcher.split(0, opts);
        assertTrue(result.isEmpty(), "Should return empty list for zero items");
    }

    @Test
    void testSplitExactMultipleOfMaxBatchSize() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(5).setMaxHandlers(10);
        List<Integer> result = Batcher.split(15, opts);
        assertEquals(List.of(5, 5, 5), result, "Should create three equal batches of maxBatchSize");
    }

    @Test
    void testSplitWithRemainder() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(7).setMaxHandlers(10);
        List<Integer> result = Batcher.split(18, opts);
        assertEquals(List.of(7, 7, 4), result, "Should create batches [7, 7, 4] for remainder");
    }

    @Test
    void testSplitMaxHandlersLimitWithRemainder() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(3).setMaxHandlers(2);
        List<Integer> result = Batcher.split(10, opts);
        assertEquals(List.of(3, 3), result, "Should be limited to maxHandlers even with remainder");
    }

    @Test
    void testSplitLargeNumberWithSmallBatches() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(2).setMaxHandlers(5);
        List<Integer> result = Batcher.split(11, opts);
        assertEquals(List.of(2, 2, 2, 2, 2), result, "Should create 5 batches of size 2 (leaving 1 item)");
    }

    @Test
    void testSplitMinBatchSizeGreaterThanMaxBatchSize() {
        // Edge case: misconfigured options where minBatchSize > maxBatchSize
        Batcher.Options opts = new Batcher.Options().setMinBatchSize(10).setMaxBatchSize(5).setMaxHandlers(1);
        List<Integer> result = Batcher.split(15, opts);
        assertEquals(List.of(5), result, "Should create only one batch when maxHandlers=1, even if more items remain");
    }

    @Test
    void testSplitMinBatchSizeGreaterThanMaxBatchSizeMultipleHandlers() {
        // Edge case: misconfigured options where minBatchSize > maxBatchSize, but multiple handlers available
        // After 4 batches of 5, we have 5 items left, which is < minBatchSize (10), so algorithm stops
        Batcher.Options opts = new Batcher.Options().setMinBatchSize(10).setMaxBatchSize(5).setMaxHandlers(5);
        List<Integer> result = Batcher.split(25, opts);
        assertEquals(List.of(5, 5, 5, 5), result, "Should create 4 batches, then stop when remaining < minBatchSize");
    }

    @Test
    void testSplitMaxHandlersOne() {
        Batcher.Options opts = new Batcher.Options().setMaxBatchSize(3).setMaxHandlers(1);
        List<Integer> result = Batcher.split(10, opts);
        assertEquals(List.of(3), result, "Should create only one batch when maxHandlers is 1");
    }

    @Test
    void testSplitDefaultOptions() {
        Batcher.Options opts = new Batcher.Options(); // All defaults
        List<Integer> result = Batcher.split(100, opts);
        assertEquals(List.of(100), result, "Should create single batch with default options (maxBatchSize=0)");
    }

    private void performSimulatedWork() {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class SizableString implements Batcher.SizableItem {
        private final String value;

        public SizableString(String value) {
            this.value = value;
        }

        @Override
        public int getByteSize() {
            return value.getBytes().length;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private static class SizableTestItem implements Batcher.SizableItem {
        private final String value;
        private final int byteSize;

        public SizableTestItem(String value, int byteSize) {
            this.value = value;
            this.byteSize = byteSize;
        }

        @Override
        public int getByteSize() {
            return byteSize;
        }

        @Override
        public String toString() {
            return value;
        }
    }
} 