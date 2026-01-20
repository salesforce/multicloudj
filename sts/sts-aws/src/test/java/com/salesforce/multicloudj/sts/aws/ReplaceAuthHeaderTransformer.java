package com.salesforce.multicloudj.sts.aws;

import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestWrapper;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReplaceAuthHeaderTransformer implements StubRequestFilterV2 {

    private static final Pattern STS_REGION_PATTERN = Pattern.compile("sts\\.(.*?)\\.amazonaws\\.com");

    @Override
    public String getName() {
        return "replace-auth-header-transformer";
    }

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
        // Only re-sign requests in record mode (when proxying to AWS)
        // In replay mode, WireMock returns recorded responses without proxying
        boolean isRecordingEnabled = System.getProperty("record") != null;
        if (!isRecordingEnabled) {
            return RequestFilterAction.continueWith(request);
        }
        
        String authHeader;
        try {
            authHeader = computeAuthHeader(request);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        Request wrappedRequest = RequestWrapper.create()
                .transformHeader( "Authorization", (input) -> Collections.singletonList(authHeader))
                .wrap(request);
        return RequestFilterAction.continueWith(wrappedRequest);
    }

    private String computeAuthHeader(Request request) throws URISyntaxException {
        SdkHttpFullRequest.Builder requestToSign =
                SdkHttpFullRequest.builder()
                        .method(SdkHttpMethod.valueOf(request.getMethod().toString()))
                        .contentStreamProvider(() -> new ByteArrayInputStream(request.getBody()))
                        .uri(new URI(request.getAbsoluteUrl()));

        requestToSign.putHeader("Content-Length", String.valueOf(request.getBody().length));
        AwsV4HttpSigner signer = AwsV4HttpSigner.create();

        // Get the region from the hostname
        Matcher matcher = STS_REGION_PATTERN.matcher(requestToSign.host());
        String region;
        if (matcher.find()) {
            region = matcher.group(1);
        } else {
            region = "us-west-2";
        }


        final SignedRequest signerOutput = signer.sign(r -> r.identity(DefaultCredentialsProvider.create().resolveCredentials())
                .request(requestToSign.build())
                .payload(requestToSign.contentStreamProvider())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "sts")
                .putProperty(AwsV4HttpSigner.REGION_NAME, region));
        
        List<String> authHeaders = signerOutput.request().headers().get("Authorization");
        if (authHeaders == null || authHeaders.isEmpty()) {
            throw new RuntimeException("Failed to generate Authorization header from signer");
        }
        return authHeaders.get(0);
    }
}