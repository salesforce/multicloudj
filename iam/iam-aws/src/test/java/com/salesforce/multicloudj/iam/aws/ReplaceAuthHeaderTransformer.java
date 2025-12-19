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

/**
 * WireMock request filter that re-signs AWS IAM requests to fix signature validation failures
 * caused by proxy interception.
 * 
 * <h2>The Problem</h2>
 * <p>
 * When the AWS SDK client is configured to route requests through a WireMock proxy, the SDK signs
 * the request with headers that include the proxy's host/port information. However, WireMock then
 * forwards the request to the actual AWS IAM endpoint (iam.amazonaws.com), creating a mismatch:
 * </p>
 * <ul>
 *   <li>The signature was computed using proxy host (e.g., localhost:8080)</li>
 *   <li>AWS receives the request at iam.amazonaws.com</li>
 *   <li>AWS signature validation fails because the Host header doesn't match the signature</li>
 * </ul>
 * 
 * <h2>The Solution</h2>
 * <p>
 * This transformer intercepts requests in the WireMock proxy and re-signs them with the correct
 * target host before forwarding to AWS:
 * </p>
 * <ol>
 *   <li>Intercepts the request before it's forwarded to AWS</li>
 *   <li>Rebuilds the request with Host header set to "iam.amazonaws.com"</li>
 *   <li>Re-signs using AWS SigV4 with credentials from ~/.aws/credentials</li>
 *   <li>Replaces all signature headers (Authorization, X-Amz-Date, etc.)</li>
 *   <li>Forwards the correctly signed request to AWS IAM</li>
 * </ol>
 * 
 * <h2>IAM-Specific Configuration</h2>
 * <ul>
 *   <li><b>Host:</b> iam.amazonaws.com (global service endpoint)</li>
 *   <li><b>Region:</b> us-east-1 (required for IAM credential scoping)</li>
 *   <li><b>Service:</b> iam (service signing name)</li>
 * </ul>
 * 
 * <h2>Usage</h2>
 * <p>Register this extension in test harness:</p>
 * <pre>
 * {@code
 * public List<String> getWiremockExtensions() {
 *     return List.of("com.salesforce.multicloudj.iam.aws.ReplaceAuthHeaderTransformer");
 * }
 * }
 * </pre>
 * 
 * <p><b>Requirements:</b> Valid AWS credentials in ~/.aws/credentials with IAM permissions.</p>
 * 
 * @see StubRequestFilterV2
 * @see AwsV4HttpSigner
 */
public class ReplaceAuthHeaderTransformer implements StubRequestFilterV2 {

    @Override
    public String getName() {
        return "replace-auth-header-transformer";
    }

    /**
     * Filters and transforms incoming requests by re-signing them with real AWS credentials.
     * 
     * <p>
     * This method is invoked by WireMock for every request passing through the proxy.
     * It intercepts the request, re-signs it with real credentials from the AWS default profile,
     * and replaces all AWS signature-related headers before forwarding to AWS IAM.
     * </p>
     * 
     * @param request the incoming HTTP request from the AWS SDK client
     * @param serveEvent the WireMock serve event context
     * @return a RequestFilterAction containing the transformed request with valid AWS signatures
     * @throws RuntimeException if URI parsing fails during re-signing
     */
    @Override
    public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
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

    /**
     * Computes a new AWS SigV4 signed request using real credentials from the default profile.
     * 
     * <p>
     * This method performs the following steps:
     * </p>
     * <ol>
     *   <li>Builds a new HTTP request from the intercepted WireMock request</li>
     *   <li>Copies non-AWS headers from the original request (excludes Authorization, Host, X-Amz-*)</li>
     *   <li>Sets the Host header to "iam.amazonaws.com" for IAM service</li>
     *   <li>Signs the request using AWS SigV4 with credentials from ~/.aws/credentials</li>
     *   <li>Returns the signed request with new Authorization and X-Amz-* headers</li>
     * </ol>
     * 
     * <p>
     * <b>Important:</b> All AWS signature-related headers (Authorization, X-Amz-Date, 
     * X-Amz-Content-SHA256, X-Amz-Security-Token, etc.) are excluded from the original request
     * and regenerated during signing to ensure signature consistency.
     * </p>
     * 
     * @param request the WireMock HTTP request to re-sign
     * @return a SignedRequest containing the newly signed request with valid AWS SigV4 headers
     * @throws URISyntaxException if the request URL cannot be parsed
     */
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
            if (!headerKey.equalsIgnoreCase("Authorization") && 
                !headerKey.equalsIgnoreCase("Host") &&
                !headerKey.startsWith("X-Amz-")) {
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
