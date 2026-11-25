package com.salesforce.multicloudj.blob;

import com.salesforce.multicloudj.blob.client.BucketClient;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.examples.AppConfig;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example: one long-lived {@code BucketClient} whose credentials come from an external source and
 * are refreshed by the SDK.
 *
 * <p>Session credentials are short lived. A client built from a fixed set of them stops working
 * when they expire, so the application would have to rebuild the client before every expiry.
 * A {@link CredentialsType#SESSION} override with a supplier removes that burden: the application
 * supplies a function that obtains fresh credentials from wherever they are issued, and the SDK
 * calls it on a schedule. The client is built once and lives for the whole application lifetime.
 *
 * <p>The provider, region and bucket come from configuration like the other blob examples (see
 * {@code examples.properties}). This example adds these keys, each also settable as a system
 * property or environment variable:
 *
 * <ul>
 *   <li>{@code refreshable.credentials.file} (required): path to a properties file holding the
 *       credentials: {@code accessKeyId}, {@code secretAccessKey} and {@code sessionToken}, plus
 *       an optional {@code expiration} as an ISO-8601 instant such as {@code
 *       2030-01-01T00:00:00Z}.
 *   <li>{@code refreshable.refresh.interval.seconds} (optional, default 30): how often the SDK
 *       calls the supplier.
 * </ul>
 *
 * <p>Run it, then rewrite the credentials file while it runs: the next refresh picks up the new
 * contents without rebuilding the client.
 */
public class RefreshableSession {
  private static final Logger logger = LoggerFactory.getLogger(RefreshableSession.class);

  private static final int LISTINGS = 12;
  private static final Duration PAUSE_BETWEEN_LISTINGS = Duration.ofSeconds(10);
  private static final long DEFAULT_REFRESH_INTERVAL_SECONDS = 30;

  public static void main(String[] args) throws InterruptedException {
    // Never hardcode provider ids or environment-specific values; resolve them from configuration
    // (see examples.properties).
    String provider = AppConfig.provider();
    String bucket = AppConfig.get("blob.bucket");
    String region = AppConfig.getOptional("region");
    Path credentialsFile = Path.of(AppConfig.get("refreshable.credentials.file"));
    Duration refreshInterval = Duration.ofSeconds(refreshIntervalSeconds());

    // The SDK may call the supplier from any thread, and a call may block while it does I/O, so
    // keep it quick and make it safe to call repeatedly.
    Supplier<StsCredentials> credentialsSupplier = () -> fetchCredentials(credentialsFile);

    // The refresh interval is required and has no default. Choose one well below the lifetime of
    // the credentials the source issues so a fresh set is always in hand before the old one
    // expires. When the supplier returns credentials that carry an expiration, the SDK refreshes
    // after the smaller of this interval and 80% of the lifetime that remains.
    CredentialsOverrider overrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentialsSupplier(credentialsSupplier)
            .withRefreshInterval(refreshInterval)
            .build();

    BucketClient.BlobBuilder builder =
        BucketClient.builder(provider).withBucket(bucket).withCredentialsOverrider(overrider);
    if (region != null) {
      builder.withRegion(region);
    }
    // Built once. The same client keeps working across every refresh below.
    BucketClient bucketClient = builder.build();

    for (int i = 1; i <= LISTINGS; i++) {
      listOnePage(bucketClient, i);
      Thread.sleep(PAUSE_BETWEEN_LISTINGS.toMillis());
    }
  }

  private static void listOnePage(BucketClient bucketClient, int attempt) {
    try {
      ListBlobsPageResponse page =
          bucketClient.listPage(ListBlobsPageRequest.builder().withMaxResults(1).build());
      logger.info(
          "listing {} succeeded, {} object(s) on the first page", attempt, page.getBlobs().size());
    } catch (SubstrateSdkException e) {
      // While the external source is unavailable the SDK keeps serving the last credentials it
      // obtained until a cutoff: twice the refresh interval for credentials without an
      // expiration, or the expiration itself when there is one. After the cutoff calls fail, and
      // after a failed attempt the SDK waits a short back-off before calling the supplier again.
      // The failure is an UnAuthorizedException, but the provider SDK may wrap it, so look
      // through the cause chain. The same type also reports credentials the cloud rejected, so
      // read the message to tell the two apart.
      if (hasCause(e, UnAuthorizedException.class)) {
        logger.warn(
            "listing {} failed: credentials unavailable or rejected: {}", attempt, e.getMessage());
      } else {
        logger.error("listing {} failed", attempt, e);
      }
    }
  }

  private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
    for (Throwable cause = error; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
      if (cause.getCause() == cause) {
        break;
      }
    }
    return false;
  }

  // Stands in for obtaining credentials from an external source. A real application would run its
  // own authentication here, for example a call to an internal credential service, and return
  // what it issues. This one re-reads a file on every call, so rewriting the file changes the
  // credentials at the next refresh.
  private static StsCredentials fetchCredentials(Path file) {
    logger.info("obtaining credentials from the external source");
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(file)) {
      properties.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read credentials from " + file, e);
    }

    // The access key id and secret are not used by every provider; some authenticate with the
    // session token alone. The SDK rejects credentials that lack any of the three, so all are
    // required here and the same file works for all providers.
    String accessKeyId = required(properties, "accessKeyId");
    String secretAccessKey = required(properties, "secretAccessKey");
    String sessionToken = required(properties, "sessionToken");

    String expiration = properties.getProperty("expiration");
    if (expiration == null || expiration.isBlank()) {
      // The source does not say when the credentials stop being valid, so the refresh interval
      // alone decides when the SDK asks again.
      return new StsCredentials(accessKeyId, secretAccessKey, sessionToken);
    }
    // The source reports the expiration, so the SDK can refresh before it arrives and never serves
    // the credentials after it.
    return new StsCredentials(
        accessKeyId, secretAccessKey, sessionToken, Instant.parse(expiration.trim()));
  }

  private static String required(Properties properties, String name) {
    String value = properties.getProperty(name);
    if (value == null || value.isBlank()) {
      throw new IllegalStateException("The credentials file is missing '" + name + "'");
    }
    return value.trim();
  }

  private static long refreshIntervalSeconds() {
    String configured = AppConfig.getOptional("refreshable.refresh.interval.seconds");
    return configured == null ? DEFAULT_REFRESH_INTERVAL_SECONDS : Long.parseLong(configured);
  }
}
