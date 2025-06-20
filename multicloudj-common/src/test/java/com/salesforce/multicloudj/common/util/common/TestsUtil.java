package com.salesforce.multicloudj.common.util.common;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.recordSpec;
import static com.salesforce.multicloudj.common.util.common.TestsUtil.TruncateRequestBodyTransformer.TRUNCATE_MATCHER_REQUST_BODY_OVER;

public class TestsUtil {
    private static Logger logger = LoggerFactory.getLogger(TestsUtil.class);
    static WireMockServer wireMockServer;
    public static final String WIREMOCK_HOST = "localhost";

    public static class TruncateRequestBodyTransformer extends StubMappingTransformer {

        public static final String TRUNCATE_MATCHER_REQUST_BODY_OVER = "TRUNCATE_MATCHER_REQUST_BODY_OVER";

        @Override
        public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {

            RequestPattern requestPattern = stubMapping.getRequest();
            List<ContentPattern<?>> bodyPatterns = requestPattern.getBodyPatterns();
            if(bodyPatterns != null && !bodyPatterns.isEmpty()) {

                List<ContentPattern<?>> newPatterns = new ArrayList<>();
                int truncateMatcherRequestBodyOver = parameters.getInt(TRUNCATE_MATCHER_REQUST_BODY_OVER);

                // See if any of the existing body patterns exceed our length limit
                for(ContentPattern<?> pattern : bodyPatterns) {
                    if(pattern.getExpected().length() > truncateMatcherRequestBodyOver){

                        // We've exceeded our desired matcher length, so truncate it
                        String truncatedString = pattern.getExpected().substring(0, truncateMatcherRequestBodyOver);
                        newPatterns.add(new RegexPattern("^" + truncatedString +"*"));
                    }
                }

                // Clear out the existing patterns so we can replace them
                if(!newPatterns.isEmpty()) {
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

    public static void startWireMockServer(String rootDir, int port, String... extensionInstances) {
        boolean isRecordingEnabled = System.getProperty("record") != null;
        logger.info("Recording enabled: {}", isRecordingEnabled);
        wireMockServer = new WireMockServer(WireMockConfiguration.options()
                .httpsPort(port)
                .port(port+1) // http port
                .keystorePath("wiremock-keystore.jks")
                .keystorePassword("password")
                .withRootDirectory(rootDir)
                .useChunkedTransferEncoding(Options.ChunkedEncodingPolicy.NEVER)
                .filenameTemplate("{{request.method}}-{{randomValue length=10}}.json")
                .extensions(new TruncateRequestBodyTransformer())
                .enableBrowserProxying(true));
        wireMockServer.start();
    }

    public static void stopWireMockServer() {
        wireMockServer.stop();
    }

    public static void startWireMockRecording(String targetEndpoint) {
        boolean isRecordingEnabled = System.getProperty("record") != null;
        RecordSpecBuilder recordSpec = recordSpec()
                // enforcing the cloud service to be always https
                .forTarget(targetEndpoint.replace("http:", "https:"))
                .extractBinaryBodiesOver(4096*2)
                .captureHeader("X-Amz-Target")
                .transformerParameters(Parameters.from(Map.of(TRUNCATE_MATCHER_REQUST_BODY_OVER,4096*2)))
                .chooseBodyMatchTypeAutomatically(true, false, false)
                .makeStubsPersistent(true);
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