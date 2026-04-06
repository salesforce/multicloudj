package com.salesforce.multicloudj.common.util.common;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.recordSpec;
import static com.salesforce.multicloudj.common.util.common.TestsUtil.TruncateRequestBodyTransformer.TRUNCATE_MATCHER_REQUEST_BODY_OVER;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Extension;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestWrapper;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.security.KeyManagementException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import lombok.Getter;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestsUtil {
  private static final Logger logger = LoggerFactory.getLogger(TestsUtil.class);
  static WireMockServer wireMockServer;
  public static final String WIREMOCK_HOST = "localhost";
  private static final String TLS_PROTOCOL = "TLS";
  private static final List<StubMappingTransformer> loadedTransformers = new ArrayList<>();
  @Getter private static String currentTestPrefix;
  private static final AtomicInteger stubCounter = new AtomicInteger(0);

  /**
   * Creates a trust manager that accepts all certificates without validation.
   * For testing purposes only.
   */
  public static TrustManager[] createTrustAllManager() {
    return new TrustManager[] {
      new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] certs, String authType) {}

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String authType) {}

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      }
    };
  }

  /**
   * Creates an SSL context that trusts all certificates. For testing purposes only.
   */
  public static SSLContext createTrustAllSSLContext() {
    try {
      SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
      sslContext.init(null, createTrustAllManager(), new SecureRandom());
      return sslContext;
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to create SSL context", e);
    }
  }

  /**
   * Gets an Apache HttpClient configured with a proxy to the WireMock server and trust-all SSL.
   * Used for services that communicate via raw HTTP (e.g., OCI registry) rather than
   * provider-specific SDK clients.
   *
   * @param port The base port for WireMock (proxy will use port+1)
   * @return A configured CloseableHttpClient
   */
  public static CloseableHttpClient getProxyHttpClient(int port) {
    SSLContext sslContext = createTrustAllSSLContext();
    return HttpClients.custom()
        .setProxy(new HttpHost(WIREMOCK_HOST, port + 1))
        .setSSLContext(sslContext)
        .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
        .build();
  }

  public static class StubNamingTransformer extends StubMappingTransformer {

    @Override
    public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
      if (currentTestPrefix != null) {
        String method = stubMapping.getRequest().getMethod().value();
        stubMapping.setName(currentTestPrefix + "-" + method + "-" + stubCounter.getAndIncrement());
      }
      return stubMapping;
    }

    @Override
    public String getName() {
      return "stub-naming-transformer";
    }
  }

  /**
   * Rewrites body matchers in recorded stubs to prevent oversized mapping files.
   *
   * <ul>
   *   <li>Binary bodies ({@link BinaryEqualToPattern}): matcher is dropped; body is verified
   *       instead via the {@code X-Body-MD5} header recorded by {@link BodyHashFilter}.
   *   <li>Long text bodies: truncated to {@value #TRUNCATE_MATCHER_REQUEST_BODY_OVER} chars and
   *       converted to a prefix regex.
   * </ul>
   */
  public static class TruncateRequestBodyTransformer extends StubMappingTransformer {

    public static final String TRUNCATE_MATCHER_REQUEST_BODY_OVER =
        "TRUNCATE_MATCHER_REQUEST_BODY_OVER";

    @Override
    public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
      RequestPattern requestPattern = stubMapping.getRequest();
      List<ContentPattern<?>> bodyPatterns = requestPattern.getBodyPatterns();
      if (bodyPatterns != null && !bodyPatterns.isEmpty()) {
        List<ContentPattern<?>> newPatterns = new ArrayList<>();
        int truncateMatcherRequestBodyOver = parameters.getInt(TRUNCATE_MATCHER_REQUEST_BODY_OVER);
        boolean modified = false;

        // See if any of the existing body patterns exceed our length limit
        for (ContentPattern<?> pattern : bodyPatterns) {
          if (pattern instanceof BinaryEqualToPattern) {
            // Drop the binary body matcher. Body correctness is verified instead via the
            // X-Body-MD5 header that BodyHashFilter injects into every request and
            // captureHeader("X-Body-MD5") records into the stub during recording.
            modified = true;
          } else if (pattern.getExpected().length() > truncateMatcherRequestBodyOver) {
            // We've exceeded our desired matcher length, so truncate it.
            // The truncated substring may start with regex metacharacters like '{',
            // so we must escape it before constructing a RegexPattern.
            String truncatedString =
                pattern.getExpected().substring(0, truncateMatcherRequestBodyOver);
            String escaped = Pattern.quote(truncatedString);
            newPatterns.add(new RegexPattern("^" + escaped + ".*"));
            modified = true;
          } else {
            // Keep the original pattern if not modified
            newPatterns.add(pattern);
          }
        }

        // Clear out the existing patterns so we can replace them
        if (modified) {
          bodyPatterns.clear();
          bodyPatterns.addAll(newPatterns);
        }
      }

      return stubMapping;
    }

    @Override
    public String getName() {
      return "truncate-request-body-transformer";
    }
  }

  /**
   * Adds an X-Query-Param-Count header matcher to each recorded stub so that during replay a stub
   * only matches requests with exactly the same number of query parameters.
   */
  public static class QueryParamCountTransformer extends StubMappingTransformer {

    @Override
    public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
      Map<String, ?> queryParams = stubMapping.getRequest().getQueryParameters();
      if (queryParams != null && !queryParams.isEmpty()) {
        int count = queryParams.size();
        RequestPatternBuilder builder = RequestPatternBuilder.like(stubMapping.getRequest());
        builder.withHeader("X-Query-Param-Count", equalTo(String.valueOf(count)));
        stubMapping.setRequest(builder.build());
      }
      return stubMapping;
    }

    @Override
    public String getName() {
      return "query-param-count-transformer";
    }
  }

  /**
   * Injects an X-Query-Param-Count header into every incoming request during replay mode so that
   * WireMock can match it against the header matcher added by {@link QueryParamCountTransformer}.
   */
  public static class QueryParamCountFilter implements StubRequestFilterV2 {

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
      if (System.getProperty("record") != null) {
        return RequestFilterAction.continueWith(request);
      }
      int count = countQueryParams(request);
      Request wrappedRequest =
          RequestWrapper.create()
              .addHeader("X-Query-Param-Count", String.valueOf(count))
              .wrap(request);
      return RequestFilterAction.continueWith(wrappedRequest);
    }

    private int countQueryParams(Request request) {
      String url = request.getUrl();
      int qIdx = url.indexOf('?');
      if (qIdx < 0 || qIdx == url.length() - 1) {
        return 0;
      }
      Set<String> paramNames = new LinkedHashSet<>();
      for (String pair : url.substring(qIdx + 1).split("&")) {
        int eqIdx = pair.indexOf('=');
        String name = eqIdx > 0 ? pair.substring(0, eqIdx) : pair;
        if (!name.isEmpty()) {
          paramNames.add(name);
        }
      }
      return paramNames.size();
    }

    @Override
    public String getName() {
      return "query-param-count-filter";
    }
  }

  /**
   * Calculates MD5 hash of request body and injects it as X-Body-MD5 header.
   * Works in both record and replay modes to enable hash-based body verification.
   * This allows us to verify request body content without storing large binary bodies in mapping
   * files.
   */
  public static class BodyHashFilter implements StubRequestFilterV2 {

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
      byte[] body = request.getBody();
      if (body != null && body.length > 0) {
        String hash = calculateMD5(body);
        Request wrappedRequest =
            RequestWrapper.create().addHeader("X-Body-MD5", hash).wrap(request);
        return RequestFilterAction.continueWith(wrappedRequest);
      }
      return RequestFilterAction.continueWith(request);
    }

    private String calculateMD5(byte[] body) {
      try {
        MessageDigest md = MessageDigest.getInstance("MD5");
        byte[] digest = md.digest(body);
        return Base64.getEncoder().encodeToString(digest);
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException("MD5 algorithm not available", e);
      }
    }

    @Override
    public String getName() {
      return "body-hash-filter";
    }
  }

  public static void startWireMockServer(String rootDir, int port, String... extensionInstances) {
    boolean isRecordingEnabled = System.getProperty("record") != null;
    logger.info("Recording enabled: {}", isRecordingEnabled);

    // Create extensions list with default transformers
    List<Extension> extensions = new ArrayList<>();
    extensions.add(new StubNamingTransformer());
    extensions.add(new QueryParamCountTransformer());
    extensions.add(new QueryParamCountFilter());
    extensions.add(new BodyHashFilter());
    extensions.add(new TruncateRequestBodyTransformer());

    // Load additional extensions if provided
    for (String extensionClass : extensionInstances) {
      try {
        Class<?> clazz = Class.forName(extensionClass);
        Extension extension = (Extension) clazz.getDeclaredConstructor().newInstance();
        extensions.add(extension);
        logger.info("Loaded WireMock extension: {}", extensionClass);

        if (extension instanceof StubMappingTransformer) {
          loadedTransformers.add((StubMappingTransformer) extension);
        }
      } catch (Exception e) {
        logger.warn("Failed to load WireMock extension: {}", extensionClass, e);
      }
    }

    wireMockServer =
        new WireMockServer(
            WireMockConfiguration.options()
                .httpsPort(port)
                .port(port + 1) // http port
                .containerThreads(100)
                .asynchronousResponseEnabled(true)
                .keystorePath("wiremock-keystore.jks")
                .keystorePassword("password")
                .withRootDirectory(rootDir)
                .gzipDisabled(true)
                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                .filenameTemplate("{{name}}.json")
                .extensions(extensions.toArray(new Extension[0]))
                .enableBrowserProxying(true));
    wireMockServer.start();
  }

  public static void stopWireMockServer() {
    wireMockServer.stop();
  }

  public static void startWireMockRecording(
      String targetEndpoint, String testClassName, String testMethodName) {
    currentTestPrefix = testClassName + "_" + testMethodName;
    stubCounter.set(0);

    boolean isRecordingEnabled = System.getProperty("record") != null;
    RecordSpecBuilder recordSpec =
        recordSpec()
            // enforcing the cloud service to be always https
            .forTarget(targetEndpoint.replace("http:", "https:"))
            .extractBinaryBodiesOver(4096 * 2)
            .captureHeader("X-Amz-Target")
            .captureHeader("X-Body-MD5")
            .transformerParameters(
                Parameters.from(
                    Map.of(
                        TRUNCATE_MATCHER_REQUEST_BODY_OVER,
                        4096 * 2)))
            .chooseBodyMatchTypeAutomatically(true, false, false)
            .makeStubsPersistent(true);

    // Apply transformers during recording
    List<String> transformerNames = new ArrayList<>();
    transformerNames.add("stub-naming-transformer");
    transformerNames.add("truncate-request-body-transformer");
    transformerNames.add("query-param-count-transformer");
    for (StubMappingTransformer t : loadedTransformers) {
      transformerNames.add(t.getName());
    }
    recordSpec = recordSpec.transformers(transformerNames.toArray(new String[0]));

    if (isRecordingEnabled) {
      wireMockServer.startRecording(recordSpec);
    }
  }

  public static void stopWireMockRecording() {
    boolean isRecordingEnabled = System.getProperty("record") != null;
    if (isRecordingEnabled) {
      wireMockServer.stopRecording();
    }
  }
}
