package com.salesforce.multicloudj.blob.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.HttpStorageOptions;
import com.google.cloud.storage.MultipartUploadClient;
import com.google.cloud.storage.MultipartUploadSettings;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.transfermanager.TransferManager;
import com.salesforce.multicloudj.blob.gcp.async.GcpAsyncBlobStore;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

class GcpBlobStoreTransportCloseTest {

  private static CredentialsOverrider credentials() {
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentials(new StsCredentials("unused", "unused", "test-token"))
        .build();
  }

  private static GcpBlobStore.Builder builder() {
    GcpBlobStore.Builder builder = new GcpBlobStore.Builder();
    builder.withBucket("test-bucket");
    builder.withCredentialsOverrider(credentials());
    builder.withIdleConnectionTimeout(Duration.ofSeconds(1));
    return builder;
  }

  @Test
  void closeStopsOwnedHttpEvictors() throws Exception {
    assertOwnedEvictorsStop(false);
  }

  @Test
  void closeStopsOwnedFallbackAndMultipartEvictorsWithGrpc() throws Exception {
    assertOwnedEvictorsStop(true);
  }

  private void assertOwnedEvictorsStop(boolean grpc) throws Exception {
    try (HttpClients clients = new HttpClients()) {
      GcpBlobStore.Builder builder = builder();
      builder.withGrpcEnabled(grpc);
      if (grpc) {
        builder.withStorage(mock(Storage.class));
      }
      try (GcpBlobStore store = builder.build()) {
        assertEquals(2, clients.evictors.size());

        store.close();
        store.close();

        clients.assertStopped();
        for (CloseableHttpClient client : clients.clients) {
          verify(client).close();
        }
      }
    }
  }

  @Test
  void closePreservesTransportOfInjectedStorage() throws Exception {
    assertInjectedTransportRemainsUsable(false);
  }

  @Test
  void closePreservesTransportOfInjectedMultipartClient() throws Exception {
    assertInjectedTransportRemainsUsable(true);
  }

  private void assertInjectedTransportRemainsUsable(boolean multipart) throws Exception {
    try (HttpClients clients = new HttpClients()) {
      CloseableHttpClient borrowed = ApacheHttpTransport.newDefaultHttpClientBuilder()
          .evictIdleConnections(1, TimeUnit.SECONDS).build();
      ApacheHttpTransport transport = new ApacheHttpTransport(borrowed);
      HttpTransportOptions transportOptions = HttpTransportOptions.newBuilder()
          .setHttpTransportFactory(() -> transport).build();
      GcpBlobStore.Builder builder = builder();
      builder.withTransferManager(mock(TransferManager.class));
      if (multipart) {
        builder.withStorage(mock(Storage.class));
        builder.withMultipartUploadClient(MultipartUploadClient.create(MultipartUploadSettings.of(
            HttpStorageOptions.http().setCredentials(NoCredentials.getInstance())
                .setTransportOptions(transportOptions).build())));
      } else {
        builder.withStorage(StorageOptions.newBuilder().setCredentials(NoCredentials.getInstance())
            .setTransportOptions(transportOptions).build().getService());
        builder.withMultipartUploadClient(mock(MultipartUploadClient.class));
      }

      try (GcpBlobStore store = builder.build()) {
        store.close();

        assertEquals(1, clients.evictors.size());
        assertTrue(clients.evictors.get(0).isAlive());
        verify(borrowed, never()).close();
      }
    }
  }

  @Test
  void asyncCloseStopsOwnedHttpEvictorsAndPreservesCallerExecutor() throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (HttpClients clients = new HttpClients()) {
      GcpAsyncBlobStore.Builder builder = GcpAsyncBlobStore.builder();
      builder.withBucket("test-bucket");
      builder.withCredentialsOverrider(credentials());
      builder.withIdleConnectionTimeout(Duration.ofSeconds(1));
      builder.withExecutorService(executor);
      try (GcpAsyncBlobStore store = builder.build()) {
        assertEquals(2, clients.evictors.size());

        store.close();

        clients.assertStopped();
        assertEquals(42, executor.submit(() -> 42).get(5, TimeUnit.SECONDS));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void builderReuseKeepsResourceOwnershipSeparate() throws Exception {
    try (HttpClients clients = new HttpClients()) {
      GcpBlobStore.Builder builder = builder();
      try (GcpBlobStore first = builder.build(); GcpBlobStore second = builder.build()) {
        assertEquals(4, clients.evictors.size());

        first.close();

        HttpClients.assertStopped(clients.evictors.subList(0, 2));
        assertTrue(clients.evictors.get(2).isAlive());
        assertTrue(clients.evictors.get(3).isAlive());

        second.close();

        clients.assertStopped();
      }
    }
  }

  @Test
  void multipartConstructionFailureClosesAlreadyCreatedTransports() throws Exception {
    IllegalStateException failure = new IllegalStateException("multipart construction failed");
    try (HttpClients clients = new HttpClients();
        MockedStatic<MultipartUploadClient> multipart = mockStatic(MultipartUploadClient.class)) {
      multipart.when(() -> MultipartUploadClient.create(any(MultipartUploadSettings.class)))
          .thenThrow(failure);

      assertSame(failure, assertThrows(IllegalStateException.class, builder()::build));

      assertEquals(2, clients.evictors.size());
      clients.assertStopped();
    }
  }

  @Test
  void transferManagerConfigurationFailureClosesTransports() throws Exception {
    try (HttpClients clients = new HttpClients()) {
      GcpBlobStore.Builder builder = builder();
      builder.withPartBufferSize(Long.MAX_VALUE);

      assertThrows(IllegalArgumentException.class, builder::build);

      assertEquals(2, clients.evictors.size());
      clients.assertStopped();
    }
  }

  @Test
  void constructorFailureClosesOwnedFallbackAndPreservesInjectedResources() throws Exception {
    Storage storage = mock(Storage.class);
    TransferManager manager = mock(TransferManager.class);
    GcpTransformerSupplier supplier = mock(GcpTransformerSupplier.class);
    IllegalStateException failure = new IllegalStateException("transformer construction failed");
    when(supplier.get("test-bucket")).thenThrow(failure);
    try (HttpClients clients = new HttpClients()) {
      GcpBlobStore.Builder builder = builder();
      builder.withStorage(storage);
      builder.withMultipartUploadClient(mock(MultipartUploadClient.class));
      builder.withTransferManager(manager);
      builder.withTransformerSupplier(supplier);
      builder.withGrpcEnabled(true);

      assertSame(failure, assertThrows(IllegalStateException.class, builder::build));

      assertEquals(1, clients.evictors.size());
      clients.assertStopped();
      verify(storage, never()).close();
      verify(manager, never()).close();
    }
  }

  @Test
  void closeAttemptsTransportsAfterSdkCleanupFailuresAndPreservesFailures() throws Exception {
    Storage storage = mock(Storage.class);
    TransferManager manager = mock(TransferManager.class);
    IOException managerFailure = new IOException("manager cleanup failed");
    IOException storageFailure = new IOException("storage cleanup failed");
    IOException httpFailure = new IOException("HTTP cleanup failed");
    doThrow(managerFailure).when(manager).close();
    doThrow(storageFailure).when(storage).close();
    try (HttpClients clients = new HttpClients()) {
      GcpBlobStore.Builder builder = builder();
      builder.withStorage(storage);
      builder.withTransferManager(manager);
      GcpBlobStore store = builder.build();
      assertEquals(1, clients.clients.size());
      doAnswer(invocation -> {
        invocation.callRealMethod();
        throw httpFailure;
      }).when(clients.clients.get(0)).close();

      SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);

      assertSame(managerFailure, thrown.getCause());
      assertEquals(2, managerFailure.getSuppressed().length);
      assertSame(storageFailure, managerFailure.getSuppressed()[0]);
      assertSame(httpFailure, managerFailure.getSuppressed()[1]);
      clients.assertStopped();
      store.close();
      verify(manager).close();
      verify(storage).close();
      verify(clients.clients.get(0)).close();
    }
  }

  @Test
  void failedBuildPreservesOriginalFailureWhenTransportCleanupFails() throws Exception {
    IllegalStateException failure = new IllegalStateException("multipart construction failed");
    IOException cleanupFailure = new IOException("HTTP cleanup failed");
    try (HttpClients clients = new HttpClients();
        MockedStatic<MultipartUploadClient> multipart = mockStatic(MultipartUploadClient.class)) {
      multipart.when(() -> MultipartUploadClient.create(any(MultipartUploadSettings.class)))
          .thenAnswer(invocation -> {
            doAnswer(close -> {
              close.callRealMethod();
              throw cleanupFailure;
            }).when(clients.clients.get(1)).close();
            throw failure;
          });

      assertSame(failure, assertThrows(IllegalStateException.class, builder()::build));

      assertEquals(1, failure.getSuppressed().length);
      assertSame(cleanupFailure, failure.getSuppressed()[0]);
      clients.assertStopped();
    }
  }

  @Test
  void closeAttemptsEverySdkClientWhenEarlierCleanupFails() throws Exception {
    Storage storage = mock(Storage.class);
    Storage fallback = mock(Storage.class);
    TransferManager manager = mock(TransferManager.class);
    IOException failure = new IOException("manager cleanup failed");
    doThrow(failure).when(manager).close();
    GcpBlobStore store = new GcpBlobStore(builder(), storage, fallback, null, manager);

    SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);

    assertSame(failure, thrown.getCause());
    InOrder order = inOrder(manager, storage, fallback);
    order.verify(manager).close();
    order.verify(storage).close();
    order.verify(fallback).close();
  }

  @Test
  void interruptedManagerCloseStillClosesStorageAndTransports() throws Exception {
    Storage storage = mock(Storage.class);
    TransferManager manager = mock(TransferManager.class);
    InterruptedException failure = new InterruptedException("manager cleanup interrupted");
    doThrow(failure).when(manager).close();
    try (HttpClients clients = new HttpClients()) {
      GcpBlobStore.Builder builder = builder();
      builder.withStorage(storage);
      builder.withTransferManager(manager);
      GcpBlobStore store = builder.build();

      try {
        SubstrateSdkException thrown = assertThrows(SubstrateSdkException.class, store::close);
        assertSame(failure, thrown.getCause());
        assertTrue(Thread.currentThread().isInterrupted());
      } finally {
        Thread.interrupted();
      }

      verify(storage).close();
      clients.assertStopped();
    }
  }

  private static class HttpClients implements AutoCloseable {
    private final List<CloseableHttpClient> clients = new ArrayList<>();
    private final List<CloseableHttpClient> delegates = new ArrayList<>();
    private final List<Thread> evictors = new ArrayList<>();
    private final MockedStatic<ApacheHttpTransport> transport =
        mockStatic(ApacheHttpTransport.class, CALLS_REAL_METHODS);

    HttpClients() {
      transport.when(ApacheHttpTransport::newDefaultHttpClientBuilder)
          .thenAnswer(invocation -> {
            HttpClientBuilder builder = spy((HttpClientBuilder) invocation.callRealMethod());
            doAnswer(build -> {
              Set<Thread> before = Thread.getAllStackTraces().keySet();
              CloseableHttpClient delegate = (CloseableHttpClient) build.callRealMethod();
              delegates.add(delegate);
              CloseableHttpClient client = spy(delegate);
              clients.add(client);
              evictors.addAll(Thread.getAllStackTraces().keySet().stream()
                  .filter(thread -> !before.contains(thread))
                  .filter(thread -> thread.getName().equals("Connection evictor"))
                  .collect(Collectors.toList()));
              return client;
            }).when(builder).build();
            return builder;
          });
    }

    void assertStopped() throws InterruptedException {
      assertStopped(evictors);
    }

    static void assertStopped(List<Thread> threads) throws InterruptedException {
      for (Thread thread : threads) {
        thread.join(TimeUnit.SECONDS.toMillis(5));
        assertFalse(thread.isAlive(), "owned HTTP evictor must terminate on cleanup");
      }
    }

    @Override
    public void close() throws IOException {
      transport.close();
      for (CloseableHttpClient client : delegates) {
        client.close();
      }
    }
  }
}
