package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ServiceException;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.aliyun.mns.common.http.ClientConfiguration;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.QueueMeta;
import com.aliyun.mns.model.SubscriptionMeta;
import com.aliyun.mns.model.TopicMeta;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alibaba SMQ (MNS) topic conformance harness: runs the shared {@link AbstractPubsubIT} suite over
 * an SMQ topic and its fan-out subscription queue via the endpoint-override recipe. A publisher
 * sends to an SMQ topic, the topic fans out to a JSON-format queue subscription, and the consumer
 * reads from that queue — so the messages arrive as the topic-delivery JSON envelope and
 * {@link AliSubscription}'s dual-mode sniff/unwrap runs against the received bytes. Because an SMQ
 * topic and its fan-out subscription queue share one MNS endpoint, the IT publishes to the topic
 * and receives the fan-out-delivered (envelope-unwrapped) message through a single
 * endpoint-override WireMock server. It does not verify the received payload end to end — the
 * shared conformance suite currently asserts only that the received body and ack id are non-null.
 *
 * <p>Modeled closely on {@link AliPubsubQueueIT} (same endpoint-override recipe, static-init
 * trust+scrub, per-driver clients, provisioning client, record-gating, scrubber, SAN keystore,
 * receive-batch-size-1). It differs only in provisioning a topic + fan-out queue + JSON
 * subscription per test and publishing through {@link AliSmqTopic}.
 *
 * <p>Runs in replay by default (no credentials) against the committed WireMock mappings; pass
 * {@code -Drecord} with session credentials on the dedicated Ali machine to regenerate them:
 * {@code mvn test -pl pubsub/pubsub-ali -Dtest=AliPubsubTopicIT -Drecord}.
 */
public class AliPubsubTopicIT extends AbstractPubsubIT {

  // Account-scoped MNS endpoint (the record target). Region is fixed, so only the account id
  // varies: the recording machine supplies SMQ_ACCOUNT_ID and we build the endpoint from
  // it. The placeholder default keeps the real account id out of committed source.
  private static final String REGION = "cn-shanghai";
  private static final String ACCOUNT_ID = envOr("SMQ_ACCOUNT_ID", "account-id");
  private static final String ENDPOINT =
      "https://" + ACCOUNT_ID + ".mns." + REGION + ".aliyuncs.com";
  // Per-test resource prefixes: topic, fan-out queue, and the topic→queue subscription binding.
  private static final String TOPIC_PREFIX = "test-smq-conf-t";
  private static final String QUEUE_PREFIX = "test-smq-conf-tq";
  private static final String SUBSCRIPTION_PREFIX = "test-smq-conf-ts";

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
  private static final String TOPIC_ALREADY_EXISTS = "TopicAlreadyExist";
  private static final String SUBSCRIPTION_ALREADY_EXISTS = "SubscriptionAlreadyExist";

  // Cap the SMQ client's connect+socket timeouts (ms) so a replay transport stall fails fast as a
  // typed SDK timeout within the test @Timeout, not an opaque 30s hang.
  private static final int SMQ_CLIENT_TIMEOUT_MILLIS = 5000;

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
    // TEMPORARY DIAGNOSTIC (revert once the CI hang is captured): start the console thread-dump
    // watchdog at class-load, before AbstractPubsubIT's @BeforeAll starts WireMock, so a hang in
    // WireMock startup or the first provisioning call is still captured. The per-test @BeforeEach
    // below re-arms it with the running method name.
    DiagnosticWatchdog.start();
    DiagnosticWatchdog.arm("class-init / @BeforeAll (WireMock start)", 180_000);
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
    // Each test gets its own topic, fan-out queue, and subscription so it never sees another
    // test's messages.
    if (harnessImpl != null) {
      harnessImpl.setNames(
          TOPIC_PREFIX + "-" + testMethodName,
          QUEUE_PREFIX + "-" + testMethodName,
          SUBSCRIPTION_PREFIX + "-" + testMethodName);
    }
    // TEMPORARY DIAGNOSTIC: (re)arm the watchdog for this test so a hang dumps every thread's
    // stack (naming the stuck method) to stdout ~2 min in, well before GitHub's 30-min step kill.
    DiagnosticWatchdog.arm("test " + testMethodName, 120_000);
  }

  @AfterEach
  public void rearmWatchdogForTeardown() {
    // TEMPORARY DIAGNOSTIC: keep a rolling budget across teardown + next-test setup so a hang
    // outside a test body (provisioning-client close, WireMock scenario reset) is also captured.
    DiagnosticWatchdog.arm("post-test teardown / next-test setup", 120_000);
  }

  public static class HarnessImpl implements Harness {
    private static final Logger logger = LoggerFactory.getLogger(AliPubsubTopicIT.class);
    private final int port = ThreadLocalRandom.current().nextInt(20000, 40000);
    private MNSClient provisioningClient;
    private String topicName = TOPIC_PREFIX;
    private String queueName = QUEUE_PREFIX;
    private String subscriptionName = SUBSCRIPTION_PREFIX;

    public void setNames(String topicName, String queueName, String subscriptionName) {
      this.topicName = topicName;
      this.queueName = queueName;
      this.subscriptionName = subscriptionName;
    }

    // The WireMock https listener that the client and provisioning both target.
    // Use 127.0.0.1 (not "localhost"): the MNS SDK's CloudTopic ctor eagerly parses the client
    // endpoint host as host.split(".")[2] for the region, so a dot-less "localhost" throws AIOOBE.
    // "127.0.0.1" has 4 dot-labels so the parse succeeds; the derived account/region are unused,
    // and 127.0.0.1 is the same loopback listener whose cert SAN includes it, so TLS/hostname
    // verification still passes.
    private URI wiremockEndpoint() {
      return URI.create("https://127.0.0.1:" + port);
    }

    // A single harness-lifetime client used ONLY to provision infra (createQueue/createTopic/
    // subscribe). It is never injected into a driver, so no per-test driver close() can shut down
    // its transport; it is closed once in close(). Each driver instead owns its own client
    // (self-built from endpoint+creds) so a per-test close() tears down only that test's client.
    private MNSClient provisioningClient() {
      if (provisioningClient == null) {
        provisioningClient =
            SmqClientFactory.buildSmqClient(wiremockEndpoint(), sessionOverriderFromEnv(), null);
      }
      return provisioningClient;
    }

    // Idempotently provision the topic, its fan-out queue, and the JSON topic→queue subscription
    // (via the provisioning client). Tolerates the *AlreadyExist* codes so re-runs are idempotent.
    private void ensureInfra() {
      MNSClient client = provisioningClient();

      QueueMeta queueMeta = new QueueMeta();
      queueMeta.setQueueName(queueName);
      createIfAbsent(() -> client.createQueue(queueMeta), QUEUE_ALREADY_EXISTS);

      TopicMeta topicMeta = new TopicMeta();
      topicMeta.setTopicName(topicName);
      createIfAbsent(() -> client.createTopic(topicMeta), TOPIC_ALREADY_EXISTS);

      SubscriptionMeta subscriptionMeta = new SubscriptionMeta();
      subscriptionMeta.setSubscriptionName(subscriptionName);
      subscriptionMeta.setEndpoint(queueResourceEndpoint());
      subscriptionMeta.setNotifyContentFormat(SubscriptionMeta.NotifyContentFormat.JSON);
      createIfAbsent(
          () -> client.getTopicRef(topicName).subscribe(subscriptionMeta),
          SUBSCRIPTION_ALREADY_EXISTS);

      // Record mode only: clear the fan-out subscription queue's settled prior-run leftovers BEFORE
      // the test publishes, so a stale topic delivery can never be captured into this run's
      // recording. Gated on the same record signal as the static-init fail-fast; in replay it is a
      // complete no-op (no drain HTTP), so the committed mappings replay green with no re-record.
      if (System.getProperty("record") != null) {
        drainQueue(queueName);
      }
    }

    // Runs a provisioning action, tolerating only the given already-exists code so re-runs are
    // idempotent; any other ServiceException propagates.
    private void createIfAbsent(Runnable action, String alreadyExistsCode) {
      try {
        action.run();
      } catch (ServiceException e) {
        if (!alreadyExistsCode.equals(e.getErrorCode())) {
          throw e;
        }
      }
    }

    // The fan-out queue's MNS resource endpoint for the subscription binding:
    // acs:mns:<region>:<accountId>:queues/<queueName>. account id and region are derived from the
    // record-target host (<accountId>.mns.<region>.aliyuncs.com), so they come from the runtime env
    // endpoint, not hardcoded source. (CloudTopic.generateQueueEndpoint is NOT used: it derives
    // from the client endpoint, which is 127.0.0.1 here.)
    private String queueResourceEndpoint() {
      String host = URI.create(ENDPOINT).getHost();
      String[] labels = host == null ? new String[0] : host.split("\\.");
      if (labels.length < 4) {
        throw new IllegalStateException(
            "Cannot derive MNS account/region from endpoint host: " + host);
      }
      String accountId = labels[0];
      String region = labels[2];
      return "acs:mns:" + region + ":" + accountId + ":queues/" + queueName;
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

    // Proves the fan-out queue empty. The drain loop already exited on an EMPTY batchPopMessage,
    // which authoritatively proves there are no visible (active) messages with no lag, because a
    // poll reads the queue directly. So this does NOT gate on the ActiveMessages count: it is
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
      ensureInfra();
      // Own client per driver (self-built from endpoint+creds, not injected) so topic.close() only
      // shuts down this test's client, never the shared provisioning client.
      AliSmqTopic.Builder builder = new AliSmqTopic.Builder();
      builder.withEndpoint(wiremockEndpoint());
      builder.withCredentialsOverrider(sessionOverriderFromEnv());
      builder.withTopicName(topicName);
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

    // Shared factory for both createSubscriptionDriver overloads. Consumes from the fan-out QUEUE
    // (not the topic), where topic deliveries land as the JSON envelope. Forces receive batch size
    // 1 for deterministic recorded stubs. Pass null to use the provider default nack timeout.
    private AbstractSubscription buildSubscription(Duration nackVisibilityTimeout) {
      ensureInfra();
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
      // Bound the SMQ client's connect+socket timeouts so a replay transport stall surfaces as a
      // fast typed SDK timeout within the test @Timeout instead of an opaque 30s hang. Endpoint-
      // override uses no proxy, so proxyEndpoint is null. Injecting before build() makes build()
      // adopt it instead of self-building one with the SDK-default (30s/40s) timeouts.
      ClientConfiguration clientConfiguration = SmqClientFactory.buildClientConfiguration(null);
      clientConfiguration.setConnectionTimeout(SMQ_CLIENT_TIMEOUT_MILLIS);
      clientConfiguration.setSocketTimeout(SMQ_CLIENT_TIMEOUT_MILLIS);
      builder.withSmqClient(
          SmqClientFactory.buildSmqClient(
              wiremockEndpoint(), sessionOverriderFromEnv(), null, clientConfiguration));
      // Populate the builder (validates, adopts the injected client, resolves the queue ref)
      // before the anonymous subclass reuses it.
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
      return AliSmqTopic.PROVIDER_ID;
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
      // try-with-resources; only the shared provisioning client is closed here. Topics/queues/
      // subscriptions are left in place (re-runs are idempotent), matching the queue harness.
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
    try (InputStream in = AliPubsubTopicIT.class.getResourceAsStream(SERVER_KEYSTORE_RESOURCE)) {
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
