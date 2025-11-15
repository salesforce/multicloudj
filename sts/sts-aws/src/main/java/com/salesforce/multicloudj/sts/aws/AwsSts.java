package com.salesforce.multicloudj.sts.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;
import software.amazon.awssdk.services.sts.model.Credentials;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityRequest;
import software.amazon.awssdk.services.sts.model.GetCallerIdentityResponse;
import software.amazon.awssdk.services.sts.model.GetSessionTokenRequest;
import software.amazon.awssdk.services.sts.model.GetSessionTokenResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("rawtypes")
@AutoService(AbstractSts.class)
public class AwsSts extends AbstractSts {

    private StsClient stsClient;

    public AwsSts(Builder builder) {
        super(builder);
        Region region = Region.of(builder.getRegion());
        StsClientBuilder sb = StsClient.builder().region(region);
        if (builder.getEndpoint() != null) {
            sb = sb.endpointOverride(builder.getEndpoint());
        }
        this.stsClient = sb.build();
    }

    public AwsSts(Builder builder, StsClient stsClient) {
        super(builder);
        this.stsClient = stsClient;
    }

    public AwsSts() {
        super(new Builder());
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request) {
        AssumeRoleRequest roleRequest = AssumeRoleRequest.builder()
                .roleArn(request.getRole())
                .roleSessionName(request.getSessionName() != null ? request.getSessionName() : "multicloudj-session-" + System.currentTimeMillis())
                .durationSeconds(request.getExpiration() != 0 ? request.getExpiration() : null)
                .build();
        AssumeRoleResponse response = stsClient.assumeRole(roleRequest);
        Credentials credentials = response.credentials();

        return new StsCredentials(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider() {
        GetCallerIdentityRequest request = GetCallerIdentityRequest.builder().build();
        GetCallerIdentityResponse response = stsClient.getCallerIdentity(request);
        return new CallerIdentity(response.userId(), response.arn(), response.account());
    }

    @Override
    protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
        GetSessionTokenRequest tokenRequest = GetSessionTokenRequest.builder()
                .durationSeconds(request.getDuration()).build();
        GetSessionTokenResponse response = stsClient.getSessionToken(tokenRequest);
        Credentials credentials = response.credentials();
        return new StsCredentials(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(
            AssumeRoleWebIdentityRequest request) {
        AssumeRoleWithWebIdentityRequest webIdentityRequest = AssumeRoleWithWebIdentityRequest.builder()
                .roleArn(request.getRole())
                .roleSessionName(request.getSessionName() != null ? request.getSessionName() : "multicloudj-web-identity-session-" + System.currentTimeMillis())
                .webIdentityToken(request.getWebIdentityToken())
                .durationSeconds(request.getExpiration() != 0 ? request.getExpiration() : null)
                .build();
        AssumeRoleWithWebIdentityResponse response = stsClient.assumeRoleWithWebIdentity(webIdentityRequest);
        Credentials credentials = response.credentials();

        return new StsCredentials(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof AwsServiceException) {
            AwsServiceException serviceException = (AwsServiceException) t;
            if (serviceException.awsErrorDetails() == null) {
                return UnknownException.class;
            }

            String errorCode = serviceException.awsErrorDetails().errorCode();
            return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
        }
        return UnknownException.class;
    }

    /**
     * The common error codes
     *
     * @see com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping
     */
    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

    static {
        Map<String, Class<? extends SubstrateSdkException>> map = new HashMap<>(CommonErrorCodeMapping.get());
        map.put("SignatureDoesNotMatch", UnAuthorizedException.class);
        ERROR_MAPPING = Collections.unmodifiableMap(map);
        // Add more mappings as needed
    }

    public static class Builder extends AbstractSts.Builder<AwsSts, Builder> {
        String param;
        protected Builder() {
            providerId("aws");
        }

        @Override
        public Builder self() {
            return this;
        }

        public Builder setParam(Map<String, String> params) {
            if (!Objects.equals(params.get("customPro"), "")) {
                param = params.get("customPro");
            }
            return this;
        }

        @Override
        public AwsSts build() {
            this.param = region;
            return new AwsSts(this);
        }

        public AwsSts build(StsClient stsClient) {
            return new AwsSts(this, stsClient);
        }
    }
}
