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
import com.github.tomakehurst.wiremock.recording.RecordSpec;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import com.github.tomakehurst.wiremock.recording.RequestBodyAutomaticPatternFactory;
import com.github.tomakehurst.wiremock.recording.RequestBodyPatternFactory;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
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

  public static class TruncateRequestBodyTransformer extends StubMappingTransformer {

    public static final String TRUNCATE_MATCHER_REQUEST_BODY_OVER =
        "TRUNCATE_MATCHER_REQUEST_BODY_OVER";

    @Override
    public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
      RequestPattern requestPattern = stubMapping.getRequest();
      List<ContentPattern<?>> bodyPatterns = requestPattern.getBodyPatterns();
      if (bodyPatterns != null && !bodyPatterns.isEmpty()) {
        List<ContentPattern<?>> newPatterns = new ArrayList<>();
        boolean changed = false;
        int truncateMatcherRequestBodyOver = parameters.getInt(TRUNCATE_MATCHER_REQUEST_BODY_OVER);

        for (ContentPattern<?> pattern : bodyPatterns) {
          boolean overLimit = pattern.getExpected().length() > truncateMatcherRequestBodyOver;
          if (overLimit && pattern instanceof BinaryEqualToPattern) {
            // Binary bodies (e.g. an octet-stream multipart part) are recorded by WireMock as a
            // BinaryEqualToPattern whose getExpected() is the BASE64 text of the raw bytes.
            // Truncating that base64 into a RegexPattern is unmatchable on replay: WireMock applies
            // a RegexPattern against the RAW request bytes, not the base64 string, so it never
            // matches and the request falls through to the live server (via browser proxying).
            // The request is already uniquely identified by method + URL + query parameters
            // (e.g. uploadId + partNumber), so drop the body matcher entirely for this case
            // rather than emit a doomed regex.
            changed = true;
            // (intentionally add nothing to newPatterns -> body matcher removed)
          } else if (overLimit) {
            // Text bodies: truncate into an anchored regex (existing behavior). The truncated
            // substring may start with regex metacharacters like '{', so quote it first.
            String truncatedString =
                pattern.getExpected().substring(0, truncateMatcherRequestBodyOver);
            String escaped = Pattern.quote(truncatedString);
            newPatterns.add(new RegexPattern("^" + escaped + ".*"));
            changed = true;
          } else {
            // Under the limit: keep the original exact matcher (binaryEqualTo / equalTo / etc).
            newPatterns.add(pattern);
          }
        }

        // Only rewrite if we actually changed something (truncated or dropped a matcher).
        if (changed) {
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

  public static void startWireMockServer(String rootDir, int port, String... extensionInstances) {
    boolean isRecordingEnabled = System.getProperty("record") != null;
    logger.info("Recording enabled: {}", isRecordingEnabled);

    // Create extensions list with default transformers
    List<Extension> extensions = new ArrayList<>();
    extensions.add(new StubNamingTransformer());
    extensions.add(new QueryParamCountTransformer());
    extensions.add(new QueryParamCountFilter());
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
                .enableBrowserProxying(true)
                .preserveHostHeader(true));
    wireMockServer.start();
  }

  public static void stopWireMockServer() {
    wireMockServer.stop();
  }

  /**
   * In replay mode, resets every WireMock stateful-scenario cursor back to its initial "Started"
   * state. Recordings are captured per-test from a fresh server, so each test's stub-walk starts at
   * "Started"; resetting before each test restores that precondition and removes cross-test
   * scenario-state contamination that otherwise makes replay depend on JUnit method order.
   * No-op during recording (scenario state is only consumed on replay).
   */
  public static void resetWireMockScenarios() {
    if (wireMockServer != null && System.getProperty("record") == null) {
      wireMockServer.resetScenarios();
    }
  }

  public static void startWireMockRecording(
      String targetEndpoint, String testClassName, String testMethodName) {
    startWireMockRecording(
        targetEndpoint, testClassName, testMethodName, List.of());
  }

  public static void startWireMockRecording(
      String targetEndpoint,
      String testClassName,
      String testMethodName,
      List<String> captureHeaders) {
    currentTestPrefix = testClassName + "_" + testMethodName;
    stubCounter.set(0);

    // Reset every stateful-scenario cursor to its initial "Started" state before each test (replay
    // only). This runs at the single chokepoint every Abstract*IT @BeforeEach calls, so it applies
    // uniformly across all services/substrates. Removes cross-test scenario-state contamination
    // that otherwise makes replay depend on JUnit method execution order.
    resetWireMockScenarios();

    boolean isRecordingEnabled = System.getProperty("record") != null;
    RecordSpecBuilder recordSpec =
        recordSpec()
            // enforcing the cloud service to be always https
            .forTarget(targetEndpoint.replace("http:", "https:"))
            .extractBinaryBodiesOver(4096 * 2)
            .captureHeader("X-Amz-Target")
            .transformerParameters(
                Parameters.from(Map.of(TRUNCATE_MATCHER_REQUEST_BODY_OVER, 4096 * 2)))
            .chooseBodyMatchTypeAutomatically(true, false, false)
            .makeStubsPersistent(true);

    for (String header : captureHeaders) {
      recordSpec = recordSpec.captureHeader(header);
    }

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
      // Rebuild the spec with a body-pattern factory that records non-text (binary) bodies
      // losslessly as base64 binaryEqualTo, wrapping WireMock's default automatic factory so text
      // providers are unaffected. There is no builder setter for a custom factory, so reconstruct
      // the RecordSpec via its public constructor, swapping only the requestBodyPatternFactory.
      RecordSpec built = recordSpec.build();
      RequestBodyPatternFactory losslessFactory =
          new LosslessBinaryBodyPatternFactory(built.getRequestBodyPatternFactory());
      RecordSpec losslessSpec =
          new RecordSpec(
              built.getTargetBaseUrl(),
              built.getFilters(),
              built.getCaptureHeaders(),
              losslessFactory,
              built.getExtractBodyCriteria(),
              built.getOutputFormat(),
              built.shouldPersist(),
              built.shouldRecordRepeatsAsScenarios(),
              built.getTransformers(),
              built.getTransformerParameters());
      wireMockServer.startRecording(losslessSpec);
    }
  }

  public static void stopWireMockRecording() {
    boolean isRecordingEnabled = System.getProperty("record") != null;
    if (isRecordingEnabled) {
      wireMockServer.stopRecording();
    }
  }

  /**
   * A recording body-pattern factory that stores non-text request bodies losslessly.
   *
   * <p>WireMock's default {@link RequestBodyAutomaticPatternFactory} decides between a text {@code
   * equalTo} matcher and a binary {@code binaryEqualTo} matcher based on the request's standard
   * {@code Content-Type}. Some providers (e.g. Alibaba Tablestore, which sends its binary
   * PlainBuffer protobuf under a custom {@code x-ots-contenttype} header) end up classified as
   * text, so the raw binary body is UTF-8 decoded into the recorded {@code equalTo} string. That
   * decode is lossy: every byte that isn't valid UTF-8 collapses to U+FFFD irreversibly, corrupting
   * the recording and making the stored body impossible to parse or reliably match.
   *
   * <p>This factory inspects the actual bytes: if the body is NOT valid UTF-8 (i.e. it is genuinely
   * binary), it emits a {@link BinaryEqualToPattern} built from the raw bytes, which serializes to
   * a lossless base64 {@code binaryEqualTo}. Otherwise it delegates to WireMock's default automatic
   * behavior, so JSON/XML/text providers are entirely unaffected. Scoping by byte content (rather
   * than by URL or provider) keeps this provider-agnostic and self-limiting.
   */
  public static class LosslessBinaryBodyPatternFactory implements RequestBodyPatternFactory {

    private final RequestBodyPatternFactory delegate;

    public LosslessBinaryBodyPatternFactory(RequestBodyPatternFactory delegate) {
      this.delegate = delegate;
    }

    @Override
    public ContentPattern<?> forRequest(Request request) {
      byte[] body = request.getBody();
      if (body != null && body.length > 0 && !isValidUtf8(body)) {
        // Genuinely binary body -> store raw bytes losslessly (base64 binaryEqualTo).
        return new BinaryEqualToPattern(body);
      }
      // Text/JSON/XML (or empty) -> keep WireMock's default automatic behavior.
      return delegate.forRequest(request);
    }

    private static boolean isValidUtf8(byte[] bytes) {
      CharsetDecoder decoder =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT);
      try {
        decoder.decode(ByteBuffer.wrap(bytes));
        return true;
      } catch (CharacterCodingException e) {
        return false;
      }
    }
  }
}
