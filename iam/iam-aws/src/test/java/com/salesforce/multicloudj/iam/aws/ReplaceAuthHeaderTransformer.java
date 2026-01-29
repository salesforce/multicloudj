package com.salesforce.multicloudj.iam.aws;

import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestWrapper;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class ReplaceAuthHeaderTransformer implements StubRequestFilterV2 {

    @Override
    public String getName() {
        return "replace-auth-header-transformer";
    }

    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
        if (System.getProperty("record") == null) {
            return RequestFilterAction.continueWith(request);
        }

        SignedRequest signedRequest;
        try {
            signedRequest = computeSignedRequest(request);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        
        // Replace all AWS signature-related headers
        RequestWrapper.Builder builder = RequestWrapper.create();
        signedRequest.request().headers().forEach((headerName, headerValues) -> {
            if (headerName.equalsIgnoreCase("Authorization") || 
                headerName.startsWith("X-Amz-") ||
                headerName.equalsIgnoreCase("Host")) {
                builder.transformHeader(headerName, (input) -> headerValues);
            }
        });
        
        Request wrappedRequest = builder.wrap(request);
        return RequestFilterAction.continueWith(wrappedRequest);
    }

    private SignedRequest computeSignedRequest(Request request) throws URISyntaxException {
        SdkHttpFullRequest.Builder requestToSign =
                SdkHttpFullRequest.builder()
                        .method(SdkHttpMethod.valueOf(request.getMethod().toString()))
                        .contentStreamProvider(() -> new ByteArrayInputStream(request.getBody()))
                        .uri(new URI(request.getAbsoluteUrl()));

        // Copy all headers from the original request except Authorization and AWS signature headers
        request.getHeaders().all().forEach(header -> {
            String headerKey = header.key();
            // Exclude all AWS signature-related headers - they will be regenerated
            if (headerKey != null &&
                !headerKey.equalsIgnoreCase("Authorization") &&
                !headerKey.equalsIgnoreCase("Host") &&
                !headerKey.toLowerCase().startsWith("x-amz-")) {
                requestToSign.putHeader(headerKey, header.values());
            }
        });
        
        // Ensure Host header is set correctly for IAM
        requestToSign.putHeader("Host", "iam.amazonaws.com");
        requestToSign.putHeader("Content-Length", String.valueOf(request.getBody().length));
        
        AwsV4HttpSigner signer = AwsV4HttpSigner.create();

        // IAM is a global service, but AWS requires us-east-1 for credential scoping
        String region = "us-east-1";

        final SignedRequest signerOutput = signer.sign(r -> r.identity(ProfileCredentialsProvider.create().resolveCredentials())
                .request(requestToSign.build())
                .payload(requestToSign.contentStreamProvider())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, "iam")
                .putProperty(AwsV4HttpSigner.REGION_NAME, region));

        return signerOutput;
    }
}
