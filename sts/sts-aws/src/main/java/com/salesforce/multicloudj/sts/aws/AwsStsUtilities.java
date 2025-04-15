package com.salesforce.multicloudj.sts.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.driver.FlowCollector;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.http.ContentStreamProvider;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.auth.aws.signer.AwsV4HttpSigner;
import software.amazon.awssdk.http.auth.spi.signer.SignedRequest;
import software.amazon.awssdk.identity.spi.AwsSessionCredentialsIdentity;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.concurrent.CompletableFuture.completedFuture;

@AutoService(AbstractStsUtilities.class)
public class AwsStsUtilities extends AbstractStsUtilities<AwsStsUtilities> {
    private static final Set<String> RESTRICTED_HEADERS = Set.of("Content-Length", "Host", "Expect");
    private static final String SIGN_STS_ENDPOINT = "https://sts.%s.amazonaws.com/";
    private static final String DEFAULT_API_ACTION_NAME = "GetCallerIdentity";
    private static final String DEFAULT_API_VERSION = "2011-06-15";
    private static final String SERVICE_SIGNING_NAME = "sts";
    private static final AwsV4HttpSigner signer = AwsV4HttpSigner.create();

    public AwsStsUtilities(Builder builder) {
        super(builder);
    }

    public AwsStsUtilities() {
        super(new Builder());
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    protected SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request) {
        // create a StsCredentials object to hold the AWS access key, secret key, and session token
        StsCredentials signingCredentials = getSigningCredentials();

        // if the unsigned request is null, create a default request
        // The default request is a POST request to the AWS STS service
        if (request == null) {
            throw new IllegalArgumentException("input request cannot be null");
        }

        // Extract the request body from the unsigned HttpRequest
        // If the request does not have a body, use HttpRequest.BodyPublishers.noBody()
        //
        // Note: We have to use the FlowCollector to collect the request body into a byte array because
        // we do not know the body contents.
        // See - https://stackoverflow.com/a/77705720
        byte[] bytes = request
                .bodyPublisher()
                .map(
                        publisher -> {
                            // collect the HttpRequest request body into bytes (if present)
                            FlowCollector<ByteBuffer> collector = new FlowCollector<ByteBuffer>();
                            publisher.subscribe(collector);
                            return collector.items().thenApply(
                                    items ->
                                            items.isEmpty() ? null : items.get(0).array()
                            );
                        })
                .orElseGet(() -> completedFuture(null)).join();

        // create a BodyPublisher from the bytes (if present); will pass it into the signer so it has the request body
        HttpRequest.BodyPublisher body;

        // If the request does not have a body, set the body to the following sts request body
        // Action=GetCallerIdentity&Version=2011-06-15
        // See - https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
        if (bytes == null) {
            bytes = ("Action=" + DEFAULT_API_ACTION_NAME + "&Version=" + DEFAULT_API_VERSION).getBytes();
        }
        final byte[] finalBytes = bytes;
        body = HttpRequest.BodyPublishers.ofByteArray(finalBytes);

        // Create the canonical request
        String canonicalUri = request.uri().getPath();
        String canonicalQuerystring = "";
        String canonicalHeaders = "content-type:" + request.headers().map().get("Content-Type") + "host:" + request.uri().getHost() + "\n";
        String signedHeaders = "content-type;host";
        String payloadHash = getSha256Hash(body.toString());
        String canonicalRequest = request.method() + "\n" + canonicalUri + "\n" + canonicalQuerystring + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash;
        String canonicalRequestHash = getSha256Hash(canonicalRequest);

        // create the stsRequest which we will sign
        HttpRequest stsRequest = createStsRequest(canonicalRequestHash);

        // Convert the above stsRequest to an aws sdk HTTP request, so it can be signed via the aws sdk
        SdkHttpFullRequest.Builder requestToSign =
                SdkHttpFullRequest.builder()
                        .method(SdkHttpMethod.valueOf(stsRequest.method()))
                        .headers(stsRequest.headers().map())
                        .uri(stsRequest.uri());

        requestToSign.contentStreamProvider(() -> new ByteArrayInputStream(finalBytes));
        requestToSign.putHeader("Content-Length", String.valueOf(bytes.length));

        // Sign the request. Some services require custom signing configuration properties (e.g. S3).
        // See AwsV4HttpSigner and AwsV4FamilyHttpSigner for the available signing options.
        Optional<ContentStreamProvider> requestPayload = Optional.ofNullable(requestToSign.contentStreamProvider());

        final AwsSessionCredentialsIdentity identity = AwsSessionCredentialsIdentity.builder()
                .accessKeyId(signingCredentials.getAccessKeyId())
                .secretAccessKey(signingCredentials.getAccessKeySecret())
                .sessionToken(signingCredentials.getSecurityToken())
                .build();

        final SignedRequest signerOutput = signer.sign(r -> r.identity(identity)
                .request(requestToSign.build())
                .payload(requestPayload.get())
                .putProperty(AwsV4HttpSigner.SERVICE_SIGNING_NAME, SERVICE_SIGNING_NAME)
                .putProperty(AwsV4HttpSigner.REGION_NAME, region));

        // extract the signed headers
        List<Map.Entry<String, List<String>>> signerOutputHeaders = signerOutput.request()
                .headers()
                .entrySet()
                .stream()
                .filter(entry -> !RESTRICTED_HEADERS.contains(entry.getKey()))
                .collect(Collectors.toList());

        // build a copy of the HttpRequest with signed headers
        HttpRequest.Builder builder = HttpRequest.newBuilder(stsRequest.uri()).method(stsRequest.method(), body);
        signerOutputHeaders.forEach(entry -> builder.setHeader(entry.getKey(), String.join(",", entry.getValue())));

        HttpRequest signed = builder.build();
        return new SignedAuthRequest(signed, signingCredentials);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (byte b : bytes) {
            result.append(String.format("%02x", b));
        }
        return result.toString();
    }

    private static String getSha256Hash(String data) {
        try {
            return bytesToHex(MessageDigest.getInstance("SHA-256").digest(data.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new UnknownException(e);
        }
    }

    private HttpRequest createStsRequest(String canonicalRequestHash) {
        // interpolate SIGN_STS_ENDPOINT string with region
        String stsEndpoint = String.format(SIGN_STS_ENDPOINT, region);
        URI uri = URI.create(stsEndpoint);
        String stsBody = "Action=" + DEFAULT_API_ACTION_NAME + "&Version=" + DEFAULT_API_VERSION;
        HttpRequest.BodyPublisher body = HttpRequest.BodyPublishers.ofByteArray(stsBody.getBytes());

        HttpRequest stsRequest = HttpRequest.newBuilder()
                .POST(body)
                .uri(uri)
                .setHeader("X-Request-Hash", canonicalRequestHash)
                .setHeader("Content-Type", "application/x-www-form-urlencoded; charset=utf-8")
                .build();
        return stsRequest;
    }

    private StsCredentials getSigningCredentials() {
        StsCredentials signingCredentials = credentialsOverrider.getSessionCredentials();

        // Determine if we need to use default credentialsOverrider
        // If the caller has not specified a StsCredentials, we will use the default credentialsOverrider provider
        if (signingCredentials == null) {
            DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();
            AwsSessionCredentials sessionCredentials = (AwsSessionCredentials) credentialsProvider.resolveCredentials();
            signingCredentials = new StsCredentials(sessionCredentials.accessKeyId(), sessionCredentials.secretAccessKey(), sessionCredentials.sessionToken());
        }
        return signingCredentials;
    }

    public static class Builder extends AbstractStsUtilities.Builder<AwsStsUtilities> {
        protected Builder() {
            providerId("aws");
        }

        @Override
        public AwsStsUtilities build() {
            return new AwsStsUtilities(this);
        }
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof AwsServiceException) {
            String errorCode = ((AwsServiceException) t).awsErrorDetails().errorCode();
            return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
        }
        return null;
    }

    // The common error codes as source of truth is here:
    // https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html
    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING = Map.of(
            "AccessDenied", UnAuthorizedException.class,
            "IncompleteSignature", InvalidArgumentException.class,
            "InternalFailure", UnknownException.class,
            "InvalidAction", InvalidArgumentException.class,
            "InvalidClientTokenId", InvalidArgumentException.class,
            "NotAuthorized", UnAuthorizedException.class,
            "OptInRequired", UnAuthorizedException.class,
            "RequestExpired", ResourceExhaustedException.class,
            "ThrottlingException", ResourceExhaustedException.class,
            "ValidationError", InvalidArgumentException.class
            // Add more mappings as needed
    );

}
