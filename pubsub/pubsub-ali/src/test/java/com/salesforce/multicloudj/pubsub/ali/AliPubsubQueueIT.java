package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.QueueMeta;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.AbstractPubsubIT;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.pubsub.driver.AbstractTopic;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alibaba SMQ (MNS) queue conformance harness: runs the shared {@link AbstractPubsubIT} suite
 * against live MNS in RECORD mode via the endpoint-override recipe. The client points its endpoint
 * straight at WireMock's https listener (a static {@code CN=localhost}/SAN localhost cert the JVM
 * trusts), and WireMock records by proxying to the real MNS endpoint — the MNS SDK has no
 * client-level TLS seam, so a per-host MITM cert cannot be used.
 *
 * <p>Runs in replay by default (no credentials) against the committed WireMock mappings; pass
 * {@code -Drecord} with session credentials on the dedicated Ali machine to regenerate them:
 * {@code mvn test -pl pubsub/pubsub-ali -Dtest=AliPubsubQueueIT -Drecord}.
 */
public class AliPubsubQueueIT extends AbstractPubsubIT {

  // Account-scoped MNS endpoint (the record target). Region is fixed, so only the account id
  // varies: the recording machine supplies SMQ_ACCOUNT_ID and we build the endpoint from
  // it. The placeholder default keeps the real account id out of committed source.
  private static final String REGION = "cn-shanghai";
  private static final String ACCOUNT_ID = envOr("SMQ_ACCOUNT_ID", "account-id");
  private static final String ENDPOINT =
      "https://" + ACCOUNT_ID + ".mns." + REGION + ".aliyuncs.com";
  private static final String BASE_QUEUE_NAME = "test-smq-conf-q";

  private static final String KEYSTORE_PASSWORD = "password";
  // JVM system properties this harness overrides to trust WireMock's localhost cert, captured and
  // restored (see @AfterAll) so the process-global mutation does not leak to other test classes.
  private static final String TRUST_STORE_PROPERTY = "javax.net.ssl.trustStore";
  private static final String TRUST_STORE_PASSWORD_PROPERTY = "javax.net.ssl.trustStorePassword";
  // Classpath resource name WireMock loads to serve its https listener cert (and that the static
  // initializer reads to build the JVM truststore anchor).
  private static final String SERVER_KEYSTORE_RESOURCE = "/pubsub-smq-localhost-keystore.jks";
  private static final String SERVER_KEYSTORE_WIREMOCK_PATH = "pubsub-smq-localhost-keystore.jks";
  private static final String LOCALHOST_ALIAS = "localhost";

  // Allowlist of benign RESPONSE headers kept in recorded stubs; every other response header (any
  // that could echo a credential or session token) is dropped. These MNS-specific names live here,
  // not in the shared scrubber/util.
  private static final String ALLOWED_RESPONSE_HEADERS =
      "x-mns-request-id,x-mns-version,x-mns-account-type,x-mns-response-format,"
          + "x-mns-threadpool-wait-time,x-mns-netty-total-time,Server,Date,Content-Type,"
          + "Content-Length,Location";

  private static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExist";

  // Drain-loop tuning for the record-mode pre-test purge: SMQ batchPopMessage caps at 16 messages
  // per call; a 1s long-poll matches the subscription wait; the round cap bounds each pop sweep so
  // a pathological queue (e.g. a concurrent producer) can never drain forever.
  private static final int DRAIN_BATCH_SIZE = 16;
  private static final int DRAIN_WAIT_SECONDS = 1;
  private static final int DRAIN_MAX_ROUNDS = 100;

  // Post-drain empty-proof tuning: the queue attribute counts (active/inactive/delay) are
  // eventually consistent, so the empty check re-reads them a bounded number of times with a short
  // pause before concluding the queue is genuinely non-empty and aborting the record run.
  private static final int DRAIN_EMPTY_CHECK_MAX_ATTEMPTS = 3;
  private static final long DRAIN_EMPTY_CHECK_SLEEP_MILLIS = 500L;

  // Original JVM truststore and scrubber system-property values captured before this harness
  // overrides them, restored in @AfterAll so the process-global mutation does not leak to other
  // test classes.
  private static String originalTrustStore;
  private static String originalTrustStorePassword;
  private static String originalAllowHeaders;
  private static String originalRedactValues;

  static {
    // In record mode a blank SMQ_ACCOUNT_ID would make the account-id redaction a no-op
    // ("account-id=account-id"), risking committing the real account id. Fail fast instead.
    String recordAccountId = System.getenv("SMQ_ACCOUNT_ID");
    if (System.getProperty("record") != null
        && (recordAccountId == null || recordAccountId.trim().isEmpty())) {
      throw new IllegalStateException(
          "SMQ_ACCOUNT_ID must be set to the real account id when recording (-Drecord); "
              + "otherwise the account-id redaction is a no-op that could commit the real id.");
    }
    originalTrustStore = System.getProperty(TRUST_STORE_PROPERTY);
    originalTrustStorePassword = System.getProperty(TRUST_STORE_PASSWORD_PROPERTY);
    originalAllowHeaders =
        System.getProperty(SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY);
    originalRedactValues =
        System.getProperty(SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY);
    // Set the JVM trust anchor (WireMock's localhost cert), the response-header allowlist, and the
    // account-id redaction BEFORE AbstractPubsubIT's @BeforeAll starts WireMock. A subclass
    // @BeforeAll runs after the superclass's, so this must happen at class-load.
    try {
      System.setProperty(TRUST_STORE_PROPERTY, buildLocalhostTrustStore());
      System.setProperty(TRUST_STORE_PASSWORD_PROPERTY, KEYSTORE_PASSWORD);
      System.setProperty(
          SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY, ALLOWED_RESPONSE_HEADERS);
      // Redact the real account id out of recorded responses (Location header, TopicURL/QueueURL,
      // TopicOwner/Subscriber) so committed mappings match the placeholder endpoint replay builds.
      System.setProperty(
          SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, ACCOUNT_ID + "=account-id");
    } catch (GeneralSecurityException | IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  @AfterAll
  public void restoreSystemProperties() {
    // Undo the process-global system-property overrides so they do not leak to other test classes.
    restoreProperty(TRUST_STORE_PROPERTY, originalTrustStore);
    restoreProperty(TRUST_STORE_PASSWORD_PROPERTY, originalTrustStorePassword);
    restoreProperty(
        SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY, originalAllowHeaders);
    restoreProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, originalRedactValues);
  }

  private static void restoreProperty(String key, String original) {
    if (original == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, original);
    }
  }

  private HarnessImpl harnessImpl;
  private String queueName;

  @Override
  protected Harness createHarness() {
    harnessImpl = new HarnessImpl();
    return harnessImpl;
  }

  @BeforeEach
  public void setupTestResources(TestInfo testInfo) {
    String testMethodName =
        testInfo
            .getTestMethod()
            .map(m -> m.getName())
            .orElseThrow(() -> new IllegalStateException("Test method not found in TestInfo"));
    // Each test gets its own queue so it never picks up another test's messages.
    queueName = BASE_QUEUE_NAME + "-" + testMethodName;
    if (harnessImpl != null) {
      harnessImpl.setQueueName(queueName);
    }
  }

  public static class HarnessImpl implements Harness {
    private static final Logger logger = LoggerFactory.getLogger(AliPubsubQueueIT.class);
    private final int port = ThreadLocalRandom.current().nextInt(20000, 40000);
    private MNSClient provisioningClient;
    private String queueName = BASE_QUEUE_NAME;

    public void setQueueName(String queueName) {
      this.queueName = queueName;
    }

    /** The WireMock https listener that the client and provisioning both target. */
    private URI wiremockEndpoint() {
      return URI.create("https://127.0.0.1:" + port);
    }

    // A single harness-lifetime client used ONLY to provision queues (createQueue). It is never
    // injected into a driver, so no per-test driver close() can shut down its transport; it is
    // closed once in close(). Each driver instead owns its own client (self-built from
    // endpoint+creds) so a per-test try-with-resources close() tears down only that test's client.
    private MNSClient provisioningClient() {
      if (provisioningClient == null) {
        provisioningClient =
            SmqClientFactory.buildSmqClient(wiremockEndpoint(), sessionOverriderFromEnv(), null);
      }
      return provisioningClient;
    }

    // Idempotently ensure the per-test queue exists (via the provisioning client). In record mode,
    // clear any settled leftovers from a prior -Drecord run BEFORE the test publishes, so a stale
    // message can never be captured into this run's recording. Gated on the same record signal as
    // the static-init fail-fast; in replay it is a complete no-op (no drain HTTP), so the committed
    // mappings replay green with no re-record.
    private void ensureQueueExists() {
      QueueMeta meta = new QueueMeta();
      meta.setQueueName(queueName);
      try {
        provisioningClient().createQueue(meta);
      } catch (ServiceException e) {
        if (!QUEUE_ALREADY_EXISTS.equals(e.getErrorCode())) {
          throw e;
        }
      }
      if (System.getProperty("record") != null) {
        drainQueue(queueName);
      }
    }

    // Record-mode pre-test drain of the named queue via the provisioning client. This is the
    // record-time integrity boundary: it must PROVE the queue empty before the test publishes so
    // stale prior-run messages can never be captured into a committed fixture. SMQ/MNS has no
    // native purge op, so this long-polls a batch of visible messages and batch-deletes their
    // receipt handles until a poll comes back empty, bounded by DRAIN_MAX_ROUNDS so it can never
    // hang. Prior-run leftovers have long since settled and are visible, so no invisibility
    // out-wait is needed. FAIL-CLOSED (record mode only): rather than best-effort continuing, it
    // aborts the record run by throwing when it cannot prove the queue drained -- when the round
    // cap is exhausted without an empty poll, a pop/delete call fails (propagated), or the
    // post-drain attribute check cannot confirm the queue empty.
    private void drainQueue(String name) {
      int removed = 0;
      CloudQueue queue = provisioningClient().getQueueRef(name);
      boolean emptyPoll = false;
      try {
        for (int round = 0; round < DRAIN_MAX_ROUNDS; round++) {
          // batchPopMessage returns null (not empty) when the queue has no visible messages.
          List<Message> messages = queue.batchPopMessage(DRAIN_BATCH_SIZE, DRAIN_WAIT_SECONDS);
          if (messages == null || messages.isEmpty()) {
            emptyPoll = true;
            break;
          }
          List<String> receiptHandles = new ArrayList<>(messages.size());
          for (Message message : messages) {
            receiptHandles.add(message.getReceiptHandle());
          }
          queue.batchDeleteMessage(receiptHandles);
          removed += messages.size();
        }
      } catch (ServiceHandlingRequiredException e) {
        // The pop/delete calls declare this checked exception; a drain failure must abort the
        // record run rather than continue, so rethrow it as an unchecked failure.
        logger.error(
            "Record-mode pre-test drain of {} failed after removing {} messages; "
                + "aborting record run to avoid a contaminated fixture",
            name, removed, e);
        throw new IllegalStateException(
            "Record-mode pre-test drain of " + name + " failed while draining messages", e);
      }
      if (!emptyPoll) {
        logger.error(
            "Record-mode pre-test drain of {} exhausted {} rounds without an empty poll "
                + "(removed {} so far); aborting record run to avoid a contaminated fixture",
            name, DRAIN_MAX_ROUNDS, removed);
        throw new IllegalStateException(
            "Record-mode pre-test drain of " + name + " could not empty the queue within "
                + DRAIN_MAX_ROUNDS + " rounds");
      }
      awaitQueueEmpty(queue, name);
      logger.info("Record-mode pre-test drain of {}: removed {} messages", name, removed);
    }

    // Proves the queue empty. The drain loop already exited on an EMPTY batchPopMessage, which
    // authoritatively proves there are no visible (active) messages with no lag, because a poll
    // reads the queue directly. So this does NOT gate on the ActiveMessages count: it is
    // eventually consistent and lags the drain's deletes, so gating on it would be redundant with
    // the empty poll AND could spuriously abort a legitimately-drained queue. It gates only on
    // the invisible (inactive) and delayed (delay) counts a poll cannot detect, re-reading them a
    // bounded number of times with a short pause to let them settle; active is still read and
    // logged for diagnostics only. A null count is treated as non-empty (fail-closed); throw to
    // abort the record run if the counts never read empty.
    private void awaitQueueEmpty(CloudQueue queue, String name) {
      Long active = null;
      Long inactive = null;
      Long delay = null;
      for (int attempt = 1; attempt <= DRAIN_EMPTY_CHECK_MAX_ATTEMPTS; attempt++) {
        QueueMeta attributes = queue.getAttributes();
        active = attributes.getActiveMessages();
        inactive = attributes.getInactiveMessages();
        delay = attributes.getDelayMessages();
        if (isZero(inactive) && isZero(delay)) {
          return;
        }
        if (attempt < DRAIN_EMPTY_CHECK_MAX_ATTEMPTS) {
          sleepBetweenEmptyChecks();
        }
      }
      logger.error(
          "Record-mode pre-test drain of {} left invisible/delayed messages (inactive={}, "
              + "delay={}, active={}); aborting record run to avoid a contaminated fixture",
          name, inactive, delay, active);
      throw new IllegalStateException(
          "Record-mode pre-test drain of " + name + " left invisible/delayed messages in the queue"
              + " (inactive=" + inactive + ", delay=" + delay + ", active=" + active + ")");
    }

    // A null count means the attribute was absent from the response; treat it as non-empty so the
    // empty-proof fails closed rather than falsely concluding the queue is drained.
    private static boolean isZero(Long count) {
      return count != null && count == 0;
    }

    // Short pause between eventual-consistency re-reads of the attribute counts. Restores the
    // interrupt flag and aborts the record run if the wait is interrupted.
    private void sleepBetweenEmptyChecks() {
      try {
        Thread.sleep(DRAIN_EMPTY_CHECK_SLEEP_MILLIS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while waiting for queue attribute counts to settle", e);
      }
    }

    @Override
    public AbstractTopic createTopicDriver() {
      ensureQueueExists();
      // Own client per driver (self-built from endpoint+creds, not injected) so topic.close() only
      // shuts down this test's client, never the shared provisioning client.
      AliSmqQueue.Builder builder = new AliSmqQueue.Builder();
      builder.withEndpoint(wiremockEndpoint());
      builder.withCredentialsOverrider(sessionOverriderFromEnv());
      builder.withTopicName(queueName);
      return builder.build();
    }

    @Override
    public AbstractSubscription createSubscriptionDriver() {
      return buildSubscription(null);
    }

    @Override
    public AbstractSubscription createSubscriptionDriver(Duration nackVisibilityTimeout) {
      return buildSubscription(nackVisibilityTimeout);
    }

    // Shared factory for both createSubscriptionDriver overloads. Forces receive batch size 1 for
    // deterministic recorded stubs. Pass null to use the provider default nack visibility timeout.
    private AbstractSubscription buildSubscription(Duration nackVisibilityTimeout) {
      ensureQueueExists();
      // Own client per driver (self-built from endpoint+creds, not injected) so a per-test
      // subscription.close() shuts down only this test's client, never the provisioning client.
      AliSubscription.Builder builder = new AliSubscription.Builder();
      builder.withEndpoint(wiremockEndpoint());
      builder.withCredentialsOverrider(sessionOverriderFromEnv());
      builder.withSubscriptionName(queueName);
      builder.withWaitTimeSeconds(1);
      if (nackVisibilityTimeout != null) {
        builder.withNackVisibilityTimeout(nackVisibilityTimeout);
      }
      // Populate the builder (validates, self-builds its client, resolves the queue ref) before the
      // anonymous subclass reuses it.
      builder.build();
      AbstractSubscription driver =
          new AliSubscription(builder) {
            @Override
            protected Batcher.Options createReceiveBatcherOptions() {
              return new Batcher.Options()
                  .setMaxHandlers(1)
                  .setMinBatchSize(1)
                  .setMaxBatchSize(1)
                  .setMaxBatchByteSize(0);
            }

            // Serialize acks the same way: batch size 1 forces one receipt handle per delete
            // request and a single handler thread, so recorded ack DELETEs replay deterministically
            // instead of racing the async client under concurrent flushes.
            @Override
            protected Batcher.Options createAckBatcherOptions() {
              return new Batcher.Options()
                  .setMaxHandlers(1)
                  .setMinBatchSize(1)
                  .setMaxBatchSize(1)
                  .setMaxBatchByteSize(0);
            }
          };
      return driver;
    }

    @Override
    public String getPubsubEndpoint() {
      return ENDPOINT;
    }

    @Override
    public String getProviderId() {
      return AliSmqQueue.PROVIDER_ID;
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of(SensitiveHeaderScrubbingTransformer.class.getName());
    }

    @Override
    public WireMockRecordingMode getWireMockRecordingMode() {
      return WireMockRecordingMode.ENDPOINT_OVERRIDE;
    }

    @Override
    public String getServerKeystorePath() {
      return SERVER_KEYSTORE_WIREMOCK_PATH;
    }

    @Override
    public void close() throws Exception {
      // Per-test drivers (and the client each owns) are closed by AbstractPubsubIT's
      // try-with-resources; only the shared provisioning client is closed here.
      if (provisioningClient != null) {
        provisioningClient.close();
      }
    }
  }

  private static CredentialsOverrider sessionOverriderFromEnv() {
    // Record mode: the ALIBABA_CLOUD_* env vars carry real session credentials. Replay mode (no
    // creds): fall back to fake values so the SMQ client still builds -- WireMock serves the stubs,
    // and if a stub miss ever falls through to the live service the fake creds fail auth loudly
    // rather than silently succeeding and masking a broken recording.
    StsCredentials credentials =
        new StsCredentials(
            envOr("ALIBABA_CLOUD_ACCESS_KEY_ID", "FAKE_ACCESS_KEY"),
            envOr("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "FAKE_SECRET_ACCESS_KEY"),
            envOr("ALIBABA_CLOUD_SECURITY_TOKEN", "FAKE_SESSION_TOKEN"));
    return new CredentialsOverrider.Builder(CredentialsType.SESSION)
        .withSessionCredentials(credentials)
        .build();
  }

  /**
   * Derives a JVM truststore anchoring exactly WireMock's static localhost cert: reads the
   * committed server keystore, extracts the {@code localhost} certificate, installs it as a
   * trusted-certificate entry in a fresh JKS temp file, and returns that file's absolute path.
   */
  private static String buildLocalhostTrustStore() throws GeneralSecurityException, IOException {
    char[] password = KEYSTORE_PASSWORD.toCharArray();
    KeyStore serverStore = KeyStore.getInstance("JKS");
    try (InputStream in = AliPubsubQueueIT.class.getResourceAsStream(SERVER_KEYSTORE_RESOURCE)) {
      if (in == null) {
        throw new IOException(SERVER_KEYSTORE_RESOURCE + " not found on the test classpath");
      }
      serverStore.load(in, password);
    }
    Certificate cert = serverStore.getCertificate(LOCALHOST_ALIAS);
    if (cert == null) {
      throw new IOException("certificate alias '" + LOCALHOST_ALIAS + "' not found");
    }
    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, password);
    trustStore.setCertificateEntry(LOCALHOST_ALIAS, cert);
    Path temp = Files.createTempFile("pubsub-smq-localhost-truststore", ".jks");
    temp.toFile().deleteOnExit();
    try (OutputStream out = Files.newOutputStream(temp)) {
      trustStore.store(out, password);
    }
    return temp.toAbsolutePath().toString();
  }

  private static String envOr(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.trim().isEmpty() ? fallback : value;
  }
}
