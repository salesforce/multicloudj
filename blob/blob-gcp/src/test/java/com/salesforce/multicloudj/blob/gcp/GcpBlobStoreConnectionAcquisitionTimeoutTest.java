package com.salesforce.multicloudj.blob.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.apache.v2.ApacheHttpTransport;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.junit.jupiter.api.Test;

class GcpBlobStoreConnectionAcquisitionTimeoutTest {

  @Test
  void poolTimeoutIsEnforcedOnStorageRequestsAndConnectionsRecover() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    AtomicInteger requests = new AtomicInteger();
    byte[] metadata = "{\"name\":\"object\",\"bucket\":\"bucket\",\"size\":\"4096\"}"
        .getBytes(StandardCharsets.UTF_8);
    server.createContext("/", exchange -> {
      requests.incrementAndGet();
      boolean hold = exchange.getRequestURI().getPath().equals("/hold");
      byte[] body = hold ? new byte[4096] : metadata;
      exchange.getResponseHeaders().set("Content-Type",
          hold ? "application/octet-stream" : "application/json");
      exchange.sendResponseHeaders(200, body.length);
      exchange.getResponseBody().write(body);
      exchange.close();
    });
    server.start();
    try {
      String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
      GcpBlobStore.Builder builder = new GcpBlobStore.Builder();
      builder.withBucket("bucket");
      builder.withEndpoint(URI.create(endpoint));
      builder.withMaxConnections(1);
      builder.withConnectionAcquisitionTimeout(Duration.ofMillis(250));
      builder.withCredentialsOverrider(new CredentialsOverrider.Builder(CredentialsType.SESSION)
          .withSessionCredentials(new StsCredentials("unused", "unused", "test-token")).build());
      StorageOptions options = GcpBlobStore.Builder.buildStorageOptions(builder);
      options = options.toBuilder().setProjectId("test-project")
          .setRetrySettings(options.getRetrySettings().toBuilder().setMaxAttempts(1).build())
          .build();
      HttpTransportOptions transportOptions = (HttpTransportOptions) options.getTransportOptions();
      ApacheHttpTransport transport =
          (ApacheHttpTransport) transportOptions.getHttpTransportFactory().create();
      try (GcpBlobStore store = new GcpBlobStore(builder, options.getService(), null, null)) {
        HttpResponse held = transport.createRequestFactory()
            .buildGetRequest(new GenericUrl(endpoint + "/hold")).execute();
        try {
          StorageException failure = assertTimeoutPreemptively(Duration.ofSeconds(5),
              () -> assertThrows(StorageException.class, () -> store.getMetadata("object", null)));

          assertInstanceOf(ConnectionPoolTimeoutException.class, failure.getCause());
          assertFalse(failure.isRetryable());
          assertEquals(1, requests.get());
          SubstrateSdkException mapped = store.mapException(failure);
          assertInstanceOf(DeadlineExceededException.class, mapped);
          assertSame(failure, mapped.getCause());
          assertTrue(mapped.isRetryable());
        } finally {
          held.disconnect();
        }

        assertEquals("object", store.getMetadata("object", null).getKey());
        assertEquals(2, requests.get());
      }
    } finally {
      server.stop(0);
    }
  }
}
