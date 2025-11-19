package com.salesforce.multicloudj.sts.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auto.service.AutoService;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.protobuf.Duration;
import com.salesforce.multicloudj.common.exceptions.DeadlineExceededException;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(AbstractSts.class)
public class GcpSts extends AbstractSts {
    private final String scope = "https://www.googleapis.com/auth/cloud-platform";
    private IamCredentialsClient stsClient;
    /**
     * Optionally injected GoogleCredentials (used primarily for testing). If null the
     * class falls back to {@code GoogleCredentials.getApplicationDefault()} at runtime.
     */
    private GoogleCredentials googleCredentials;

    public GcpSts(Builder builder) {
        super(builder);
        try {
            this.stsClient = IamCredentialsClient.create();
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create IAM client ", e);
        }
    }

    public GcpSts(Builder builder, IamCredentialsClient stsClient) {
        super(builder);
        this.stsClient = stsClient;
    }

    public GcpSts(Builder builder, IamCredentialsClient stsClient, GoogleCredentials credentials) {
        super(builder);
        this.stsClient = stsClient;
        this.googleCredentials = credentials;
    }

    public GcpSts() {
        super(new Builder());
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request){
        GenerateAccessTokenRequest.Builder accessTokenRequestBuilder = GenerateAccessTokenRequest.newBuilder()
                .setName("projects/-/serviceAccounts/" + request.getRole())
                .addAllScope(List.of(scope));
        if (request.getExpiration() > 0) {
            accessTokenRequestBuilder.setLifetime(Duration.newBuilder().setSeconds(request.getExpiration()));
        }
        GenerateAccessTokenResponse response = this.stsClient.generateAccessToken(accessTokenRequestBuilder.build());
        return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, response.getAccessToken());
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider(GetCallerIdentityRequest request) {
        try {
            GoogleCredentials credentials = getCredentials();
            credentials.refreshIfExpired();
            IdTokenCredentials idTokenCredentials =
                    IdTokenCredentials.newBuilder()
                            .setIdTokenProvider((IdTokenProvider) credentials)
                            .setTargetAudience(request.getAud() != null ? request.getAud().toLowerCase() : "multicloudj")
                            .build();
            String idToken = idTokenCredentials.refreshAccessToken().getTokenValue();

            return new CallerIdentity(StringUtils.EMPTY, idToken, StringUtils.EMPTY);
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create credentials in given environment", e);
        }
    }

    @Override
    protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
        try {
            GoogleCredentials credentials = getCredentials();
            credentials.refreshIfExpired();
            return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, credentials.getAccessToken().getTokenValue());
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create credentials in given environment", e);
        }
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(AssumeRoleWebIdentityRequest request) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    private GoogleCredentials getCredentials() {
        if (googleCredentials != null) {
            return googleCredentials;
        }
        try {
            return GoogleCredentials.getApplicationDefault().createScoped(List.of(scope));
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create credentials in given environment", e);
        }
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof ApiException) {
            ApiException exception = (ApiException) t;
            StatusCode statusCode = exception.getStatusCode();
            return ERROR_MAPPING.getOrDefault(statusCode.getCode(), UnknownException.class);
        }
        return UnknownException.class;
    }

    private static final Map<StatusCode.Code, Class<? extends SubstrateSdkException>> ERROR_MAPPING = new HashMap<>();
    static {
        ERROR_MAPPING.put(StatusCode.Code.CANCELLED, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNKNOWN, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
        ERROR_MAPPING.put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
        ERROR_MAPPING.put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
        ERROR_MAPPING.put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
        ERROR_MAPPING.put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
        ERROR_MAPPING.put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
        ERROR_MAPPING.put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
        ERROR_MAPPING.put(StatusCode.Code.ABORTED, DeadlineExceededException.class);
        ERROR_MAPPING.put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
        ERROR_MAPPING.put(StatusCode.Code.INTERNAL, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNAVAILABLE, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.DATA_LOSS, UnknownException.class);
        ERROR_MAPPING.put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
    }

    public static class Builder extends AbstractSts.Builder<GcpSts, Builder> {
        protected Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        @Override
        public Builder self() {
            return this;
        }

        public GcpSts build(IamCredentialsClient stsClient, GoogleCredentials credentials) {
            return new GcpSts(this, stsClient, credentials);
        }

        public GcpSts build(IamCredentialsClient stsClient) {
            return new GcpSts(this, stsClient);
        }

        @Override
        public GcpSts build() {
            return new GcpSts(this);
        }
    }
}
