package com.salesforce.multicloudj.pubsub.ali;

import com.aliyun.mns.client.CloudQueue;
import com.aliyun.mns.client.MNSClient;
import com.aliyun.mns.common.ServiceHandlingRequiredException;
import com.aliyun.mns.model.Message;
import com.aliyun.mns.model.QueueMeta;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.pubsub.batcher.Batcher;
import com.salesforce.multicloudj.pubsub.client.AbstractPubsubIT;
import com.salesforce.multicloudj.pubsub.driver.AbstractSubscription;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared base for the Alibaba SMQ (MNS) conformance harnesses ({@link AliPubsubQueueIT} and
 * {@link AliPubsubTopicIT}). It holds everything the two ITs run identically: the account-scoped
 * endpoint and drain tuning constants, the process-global scrubber system-property setup/teardown,
 * the record-mode fail-closed queue drain, the SMQ session-credential builder, and the subscription
 * plumbing (a receive-batch-size-1 {@link AliSubscription} over a single WireMock forward proxy).
 *
 * <p>Subclasses supply only the parts that differ: the concrete provisioning of their per-test
 * resources ({@link AbstractHarness#ensureResources()}), the queue a subscription consumes from
 * ({@link AbstractHarness#subscriptionQueueName()}), the topic-side driver
 * ({@link Harness#createTopicDriver()}), and the provider id ({@link Harness#getProviderId()}).
 */
public abstract class AbstractAliPubsubIT extends AbstractPubsubIT {

  // Account-scoped MNS endpoint (the record target). Region is fixed, so only the account id
  // varies: the recording machine supplies SMQ_ACCOUNT_ID and we build the endpoint from
  // it. The placeholder default keeps the real account id out of committed source.
  protected static final String REGION = "cn-shanghai";
  protected static final String ACCOUNT_ID = envOr("SMQ_ACCOUNT_ID", "account-id");
  protected static final String ENDPOINT =
      "http://" + ACCOUNT_ID + ".mns." + REGION + ".aliyuncs.com";

  // Allowlist of benign RESPONSE headers kept in recorded stubs; every other response header (any
  // that could echo a credential or session token) is dropped. These MNS-specific names live here,
  // not in the shared scrubber/util.
  protected static final String ALLOWED_RESPONSE_HEADERS =
      "x-mns-request-id,x-mns-version,x-mns-account-type,x-mns-response-format,"
          + "x-mns-threadpool-wait-time,x-mns-netty-total-time,Server,Date,Content-Type,"
          + "Content-Length,Location";

  protected static final String QUEUE_ALREADY_EXISTS = "QueueAlreadyExist";

  // Drain-loop tuning for the record-mode pre-test purge: SMQ batchPopMessage caps at 16 messages
  // per call; a 1s long-poll matches the subscription wait; the round cap bounds each pop sweep so
  // a pathological queue (e.g. a concurrent producer) can never drain forever.
  protected static final int DRAIN_BATCH_SIZE = 16;
  protected static final int DRAIN_WAIT_SECONDS = 1;
  protected static final int DRAIN_MAX_ROUNDS = 100;

  // Post-drain empty-proof tuning: the queue attribute counts (active/inactive/delay) are
  // eventually consistent, so the empty check re-reads them a bounded number of times with a short
  // pause before concluding the queue is genuinely non-empty and aborting the record run.
  protected static final int DRAIN_EMPTY_CHECK_MAX_ATTEMPTS = 3;
  protected static final long DRAIN_EMPTY_CHECK_SLEEP_MILLIS = 500L;

  // Original scrubber system-property values captured before this harness overrides them, restored
  // in @AfterAll so the process-global mutation does not leak to other test classes.
  private String originalAllowHeaders;
  private String originalRedactValues;

  // Whether installScrubberProperties() completed, so @AfterAll only restores what it actually set
  // -- @AfterAll runs even if this or a superclass @BeforeAll threw.
  private boolean scrubberPropertiesInstalled;

  // Install the scrubber system properties once per concrete IT class. AbstractPubsubIT is
  // @TestInstance(PER_CLASS), so this non-static @BeforeAll runs once per subclass, so both ITs
  // record with scrubbing enabled even when the failsafe fork is reused across classes. (A
  // class-load static block plus a per-class @AfterAll restore would leave the second IT in a
  // reused fork recording unscrubbed.) The scrubber reads these properties fresh at
  // transform()/stopRecording time (record mode only), not at WireMock start, so installing them
  // here rather than before the superclass starts WireMock is sufficient.
  @BeforeAll
  public void installScrubberProperties() {
    // In record mode a blank SMQ_ACCOUNT_ID would make the account-id redaction a no-op
    // ("account-id=account-id"), risking committing the real account id. Fail fast instead.
    String recordAccountId = System.getenv("SMQ_ACCOUNT_ID");
    if (System.getProperty("record") != null
        && (recordAccountId == null || recordAccountId.trim().isEmpty())) {
      throw new IllegalStateException(
          "SMQ_ACCOUNT_ID must be set to the real account id when recording (-Drecord); "
              + "otherwise the account-id redaction is a no-op that could commit the real id.");
    }
    originalAllowHeaders =
        System.getProperty(SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY);
    originalRedactValues =
        System.getProperty(SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY);
    // Set the response-header allowlist so only benign headers are kept in recorded stubs; every
    // other response header (any that could echo a credential or session token) is dropped.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.ALLOW_HEADERS_PROPERTY, ALLOWED_RESPONSE_HEADERS);
    // Redact the real account id out of recorded responses (Location header, TopicURL/QueueURL,
    // TopicOwner/Subscriber) so committed mappings match the placeholder endpoint replay builds.
    System.setProperty(
        SensitiveHeaderScrubbingTransformer.REDACT_VALUES_PROPERTY, ACCOUNT_ID + "=account-id");
    scrubberPropertiesInstalled = true;
  }

  @AfterAll
  public void restoreScrubberProperties() {
    // Install didn't complete -- via our own validation or a preceding superclass @BeforeAll
    // failure -- so there is nothing of ours to restore; don't clear caller/pre-existing values.
    if (!scrubberPropertiesInstalled) {
      return;
    }
    // Undo the process-global system-property overrides so they do not leak to other test classes.
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

  /**
   * Shared harness scaffolding for the SMQ ITs: the random WireMock port, the account/proxy
   * endpoints, the single provisioning client, the record-mode fail-closed drain, and the
   * receive-batch-size-1 subscription plumbing. Subclasses implement {@link #ensureResources()} to
   * provision their per-test resources and {@link #subscriptionQueueName()} to name the queue a
   * subscription consumes from, plus the interface's {@link #createTopicDriver()} and
   * {@link #getProviderId()}.
   */
  public abstract static class AbstractHarness implements Harness {
    protected static final Logger logger = LoggerFactory.getLogger(AbstractHarness.class);
    private final int port = ThreadLocalRandom.current().nextInt(20000, 40000);
    private MNSClient provisioningClient;

    // The real account-scoped MNS endpoint the client and provisioning both target (over plain
    // HTTP); WireMock intercepts each request via the forward proxy below.
    protected URI accountEndpoint() {
      return URI.create(ENDPOINT);
    }

    // WireMock's HTTP listener (base port + 1), used as the forward proxy — the same transport the
    // shared TestsUtil.startWireMockServer path sets up. In replay the placeholder account host
    // never resolves, so a stub miss fails loud instead of reaching live MNS.
    protected URI proxyEndpoint() {
      return URI.create("http://" + TestsUtil.WIREMOCK_HOST + ":" + (port + 1));
    }

    // A single harness-lifetime client used ONLY to provision resources. It is never injected into
    // a driver, so no per-test driver close() can shut down its transport; it is closed once in
    // close(). Each driver instead owns its own client (self-built from endpoint+creds) so a
    // per-test try-with-resources close() tears down only that test's client.
    protected MNSClient provisioningClient() {
      if (provisioningClient == null) {
        provisioningClient =
            SmqClientFactory.buildSmqClient(
                accountEndpoint(), sessionOverriderFromEnv(), proxyEndpoint());
      }
      return provisioningClient;
    }

    // Idempotently provision this harness's per-test resources (via the provisioning client), and
    // in record mode drain any settled prior-run leftovers BEFORE the test publishes so a stale
    // message can never be captured into this run's recording. In replay it is a complete no-op (no
    // drain HTTP), so the committed mappings replay green with no re-record.
    protected abstract void ensureResources();

    // The queue a subscription consumes from: the queue itself for the queue harness, the fan-out
    // subscription queue for the topic harness. Read by buildSubscription to name the subscription.
    protected abstract String subscriptionQueueName();

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
    protected void drainQueue(String name) {
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
    public AbstractSubscription createSubscriptionDriver() {
      return buildSubscription(null);
    }

    @Override
    public AbstractSubscription createSubscriptionDriver(Duration nackVisibilityTimeout) {
      return buildSubscription(nackVisibilityTimeout);
    }

    // Shared factory for both createSubscriptionDriver overloads. Consumes from the subclass's
    // subscription queue, forcing receive batch size 1 for deterministic recorded stubs. Pass null
    // to use the provider default nack visibility timeout.
    private AbstractSubscription buildSubscription(Duration nackVisibilityTimeout) {
      ensureResources();
      // Own client per driver (self-built from endpoint+creds, not injected) so a per-test
      // subscription.close() shuts down only this test's client, never the provisioning client.
      AliSubscription.Builder builder = new AliSubscription.Builder();
      builder.withEndpoint(accountEndpoint());
      builder.withProxyEndpoint(proxyEndpoint());
      builder.withCredentialsOverrider(sessionOverriderFromEnv());
      builder.withSubscriptionName(subscriptionQueueName());
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
    public int getPort() {
      return port;
    }

    @Override
    public List<String> getWiremockExtensions() {
      return List.of(SensitiveHeaderScrubbingTransformer.class.getName());
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

  protected static CredentialsOverrider sessionOverriderFromEnv() {
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

  protected static String envOr(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.trim().isEmpty() ? fallback : value;
  }
}
