package com.salesforce.multicloudj.blob.gcp.async;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.MultipartUploadClient;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.google.cloud.storage.transfermanager.TransferManagerConfig;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.DirectoryUploadRequest;
import com.salesforce.multicloudj.blob.gcp.GcpBlobStore;
import com.salesforce.multicloudj.blob.gcp.GcpTransformerSupplier;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class GcpAsyncBlobStoreCloseTest {

  @Test
  void closeTerminatesTransferWorkersAndPreservesCallerExecutor(@TempDir Path directory)
      throws Exception {
    Storage storage = mock(Storage.class);
    StorageOptions options =
        StorageOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .setServiceFactory(ignored -> storage)
            .build();
    AtomicReference<Thread> worker = new AtomicReference<>();
    Blob uploaded = mock(Blob.class);
    when(uploaded.asBlobInfo()).thenReturn(BlobInfo.newBuilder("test-bucket", "data.txt").build());
    when(storage.createFrom(
            any(BlobInfo.class), any(Path.class), any(Storage.BlobWriteOption[].class)))
        .thenAnswer(
            invocation -> {
              worker.set(Thread.currentThread());
              return uploaded;
            });
    Files.writeString(directory.resolve("data.txt"), "content");

    ExecutorService callerExecutor = Executors.newSingleThreadExecutor();
    try (TransferManager manager =
        TransferManagerConfig.newBuilder()
            .setStorageOptions(options)
            .setMaxWorkers(1)
            .build()
            .getService()) {
      GcpBlobStore wrapped =
          new GcpBlobStore.Builder()
              .withStorage(storage)
              .withMultipartUploadClient(mock(MultipartUploadClient.class))
              .withTransferManager(manager)
              .withBucket("test-bucket")
              .build();
      GcpAsyncBlobStore.Builder builder = GcpAsyncBlobStore.builder().withGcpBlobStore(wrapped);
      builder.withExecutorService(callerExecutor);
      GcpAsyncBlobStore store = builder.build();

      assertTrue(
          store
              .uploadDirectory(
                  DirectoryUploadRequest.builder()
                      .localSourceDirectory(directory.toString())
                      .build())
              .get(5, TimeUnit.SECONDS)
              .getFailedTransfers()
              .isEmpty());
      assertTrue(worker.get().isAlive());

      store.close();

      worker.get().join(TimeUnit.SECONDS.toMillis(5));
      assertFalse(worker.get().isAlive());
      assertEquals(42, callerExecutor.submit(() -> 42).get(5, TimeUnit.SECONDS));
      verify(storage).close();
    } finally {
      callerExecutor.shutdownNow();
    }
  }

  @Test
  void closeClosesSharedStorageOnce() throws Exception {
    Storage storage = mock(Storage.class);
    TransferManager manager = mock(TransferManager.class);
    GcpBlobStore wrapped = new GcpBlobStore(new GcpBlobStore.Builder(), storage, null, manager);
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, storage, new GcpTransformerSupplier());

    store.close();
    store.close();

    verify(manager).close();
    verify(storage).close();
    assertEquals(42, ForkJoinPool.commonPool().submit(() -> 42).get(5, TimeUnit.SECONDS));
  }

  @Test
  void closeClosesSharedHttpFallbackOnce() throws Exception {
    Storage storage = mock(Storage.class);
    Storage httpStorage = mock(Storage.class);
    GcpBlobStore wrapped =
        new GcpBlobStore(new GcpBlobStore.Builder(), storage, httpStorage, null, null);
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, httpStorage, new GcpTransformerSupplier());

    store.close();

    verify(storage).close();
    verify(httpStorage).close();
  }

  @Test
  void closeClosesDistinctDirectStorage() throws Exception {
    Storage storage = mock(Storage.class);
    Storage directStorage = mock(Storage.class);
    GcpBlobStore wrapped = new GcpBlobStore(new GcpBlobStore.Builder(), storage, null, null);
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, directStorage, new GcpTransformerSupplier());

    store.close();

    verify(storage).close();
    verify(directStorage).close();
  }

  @Test
  void closeAttemptsSharedStorageWhenTransferManagerFails() throws Exception {
    Storage storage = mock(Storage.class);
    Storage httpStorage = mock(Storage.class);
    TransferManager manager = mock(TransferManager.class);
    IOException failure = new IOException("transfer manager cleanup failed");
    doThrow(failure).when(manager).close();
    GcpBlobStore wrapped =
        new GcpBlobStore(new GcpBlobStore.Builder(), storage, httpStorage, null, manager);
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, storage, new GcpTransformerSupplier());

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);

    assertSame(failure, thrown.getCause().getCause());
    verify(storage).close();
    verify(httpStorage).close();
  }

  @Test
  void closePreservesWrappedFailureWhenDirectStorageAlsoFails() throws Exception {
    AbstractBlobStore wrapped = mock(AbstractBlobStore.class);
    Storage storage = mock(Storage.class);
    IOException wrappedFailure = new IOException("wrapped cleanup failed");
    IOException directFailure = new IOException("direct cleanup failed");
    doThrow(wrappedFailure).when(wrapped).close();
    doThrow(directFailure).when(storage).close();
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, storage, new GcpTransformerSupplier());

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);
    assertDoesNotThrow(store::close);

    assertSame(wrappedFailure, thrown.getCause());
    assertEquals(1, wrappedFailure.getSuppressed().length);
    assertSame(directFailure, wrappedFailure.getSuppressed()[0]);
    verify(wrapped, times(1)).close();
    verify(storage, times(1)).close();
  }

  @Test
  void closePreservesDirectStorageFailure() throws Exception {
    AbstractBlobStore wrapped = mock(AbstractBlobStore.class);
    Storage storage = mock(Storage.class);
    IOException failure = new IOException("direct cleanup failed");
    doThrow(failure).when(storage).close();
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, storage, new GcpTransformerSupplier());

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);

    assertSame(failure, thrown.getCause());
    verify(wrapped).close();
  }

  @Test
  void closeAttemptsDirectStorageWhenWrappedStoreThrowsError() throws Exception {
    AbstractBlobStore wrapped = mock(AbstractBlobStore.class);
    Storage storage = mock(Storage.class);
    AssertionError failure = new AssertionError("wrapped cleanup failed");
    doThrow(failure).when(wrapped).close();
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, storage, new GcpTransformerSupplier());

    assertSame(failure, assertThrows(AssertionError.class, store::close));

    verify(storage).close();
  }

  @Test
  void closePreservesStorageFailureAndAttemptsHttpFallback() throws Exception {
    Storage storage = mock(Storage.class);
    Storage httpStorage = mock(Storage.class);
    IOException storageFailure = new IOException("storage cleanup failed");
    IOException httpFailure = new IOException("HTTP cleanup failed");
    doThrow(storageFailure).when(storage).close();
    doThrow(httpFailure).when(httpStorage).close();
    GcpBlobStore wrapped =
        new GcpBlobStore(new GcpBlobStore.Builder(), storage, httpStorage, null, null);
    GcpAsyncBlobStore store =
        new GcpAsyncBlobStore(wrapped, null, storage, new GcpTransformerSupplier());

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);

    SubstrateSdkException wrappedFailure =
        assertInstanceOf(SubstrateSdkException.class, thrown.getCause());
    assertSame(storageFailure, wrappedFailure.getCause());
    assertEquals(1, storageFailure.getSuppressed().length);
    assertSame(httpFailure, storageFailure.getSuppressed()[0]);
    verify(storage).close();
    verify(httpStorage).close();
  }
}
