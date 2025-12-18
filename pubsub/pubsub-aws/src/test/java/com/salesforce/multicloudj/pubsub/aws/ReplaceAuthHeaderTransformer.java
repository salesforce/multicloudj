package com.salesforce.multicloudj.pubsub.aws;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * WireMock transformer that replaces the Authorization header with a newly computed one.
 * 
 * This is needed because when WireMock proxies requests to AWS services, the original
 * signature becomes invalid. This transformer re-signs the request using the actual
 * AWS credentials before forwarding to the real AWS service.
 * 
 * Supports both SNS and SQS services by detecting the service from the hostname.
 */
public class ReplaceAuthHeaderTransformer implements StubRequestFilterV2 {

    @Override
    public String getName() {
        return "replace-auth-header-transformer";
    }

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
        // Only process requests to AWS services
        String url = request.getAbsoluteUrl();
        if (url == null || (!url.contains("amazonaws.com") && !url.contains("sns") && !url.contains("sqs"))) {
            return RequestFilterAction.continueWith(request);
        }
        
        String authHeader;
        try {
            authHeader = computeAuthHeader(request);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to compute auth header for: " + url, e);
        } catch (Exception e) {
            // If signing fails, log and continue with original request
            System.err.println("Warning: Failed to re-sign request: " + e.getMessage());
            return RequestFilterAction.continueWith(request);
        }
        Request wrappedRequest = RequestWrapper.create()
                .transformHeader("Authorization", (input) -> Collections.singletonList(authHeader))
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
        
        // Copy all headers from the original request
        request.getHeaders().all().forEach(header -> {
            if (!header.key().equalsIgnoreCase("Authorization") && 
                !header.key().equalsIgnoreCase("Content-Length")) {
                requestToSign.putHeader(header.key(), header.values());
            }
        });

        AwsV4HttpSigner signer = AwsV4HttpSigner.create();

        // Detect service and region from the hostname
        String hostname = requestToSign.host();
        String serviceName;
        String region;
        
        // Match patterns like: sns.us-west-2.amazonaws.com or sqs.us-west-2.amazonaws.com
        Pattern pattern = Pattern.compile("(sns|sqs)\\.([^.]+)\\.amazonaws\\.com");
        Matcher matcher = pattern.matcher(hostname);
        if (matcher.find()) {
            serviceName = matcher.group(1);
            region = matcher.group(2);
        } else {
            // Fallback: default to us-west-2 
            region = "us-west-2";
            // Default to sns if we can't determine from hostname
            serviceName = hostname.contains("sns") ? "sns" : "sqs";
        }

        final SignedRequest signerOutput = signer.sign(r -> r.identity(DefaultCredentialsProvider.builder().build().resolveCredentials())
                .request(requestToSign.build())
                .payload(requestToSign.contentStreamProvider())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, serviceName)
                .putProperty(AwsV4HttpSigner.REGION_NAME, region));
        return signerOutput.request().headers().get("Authorization").get(0);
    }
}

