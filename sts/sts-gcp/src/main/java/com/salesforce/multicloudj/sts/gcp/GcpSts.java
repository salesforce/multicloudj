package com.salesforce.multicloudj.sts.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
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
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
@AutoService(AbstractSts.class)
public class GcpSts extends AbstractSts<GcpSts> {
    private final String scope = "https://www.googleapis.com/auth/cloud-platform";
    private IamCredentialsClient stsClient;

    public GcpSts(Builder builder) {
        super(builder);
        try {
            stsClient = IamCredentialsClient.create();
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create IAM client ", e);
        }
    }

    public GcpSts(Builder builder, IamCredentialsClient stsClient) {
        super(builder);
        this.stsClient = stsClient;
    }

    public GcpSts() {
        super(new Builder());
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request){
        GenerateAccessTokenRequest accessTokenRequest = GenerateAccessTokenRequest.newBuilder()
                .setName("projects/-/serviceAccounts/" + request.getRole())
                .addAllScope(List.of(scope))
                .setLifetime(Duration.newBuilder().setSeconds(request.getExpiration()).build())
                .build();
        GenerateAccessTokenResponse response = this.stsClient.generateAccessToken(accessTokenRequest);
        return new StsCredentials(null, null, response.getAccessToken());
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider() {
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault().createScoped(List.of(scope));
            credentials.refreshIfExpired();
            return new CallerIdentity(null, credentials.getAccessToken().getTokenValue(), null);
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create credentials in given environment", e);
        }
    }

    @Override
    protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
        try {
            GoogleCredentials credentials = GoogleCredentials.getApplicationDefault().createScoped(List.of(scope));
            credentials.refreshIfExpired();
            return new StsCredentials(null, null, credentials.getAccessToken().getTokenValue());
        } catch (IOException e) {
            throw new SubstrateSdkException("Could not create credentials in given environment", e);
        }
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof ApiException) {
            ApiException exception = (ApiException) t;
            StatusCode statusCode = exception.getStatusCode();

            Map<StatusCode.Code, Class<? extends SubstrateSdkException>> ERROR_MAP = new HashMap<>() {{
                put(StatusCode.Code.CANCELLED, UnknownException.class);
                put(StatusCode.Code.UNKNOWN, UnknownException.class);
                put(StatusCode.Code.INVALID_ARGUMENT, InvalidArgumentException.class);
                put(StatusCode.Code.DEADLINE_EXCEEDED, DeadlineExceededException.class);
                put(StatusCode.Code.NOT_FOUND, ResourceNotFoundException.class);
                put(StatusCode.Code.ALREADY_EXISTS, ResourceAlreadyExistsException.class);
                put(StatusCode.Code.PERMISSION_DENIED, UnAuthorizedException.class);
                put(StatusCode.Code.RESOURCE_EXHAUSTED, ResourceExhaustedException.class);
                put(StatusCode.Code.FAILED_PRECONDITION, FailedPreconditionException.class);
                put(StatusCode.Code.ABORTED, DeadlineExceededException.class);
                put(StatusCode.Code.OUT_OF_RANGE, InvalidArgumentException.class);
                put(StatusCode.Code.UNIMPLEMENTED, UnSupportedOperationException.class);
                put(StatusCode.Code.INTERNAL, UnknownException.class);
                put(StatusCode.Code.UNAVAILABLE, UnknownException.class);
                put(StatusCode.Code.DATA_LOSS, UnknownException.class);
                put(StatusCode.Code.UNAUTHENTICATED, UnAuthorizedException.class);
            }};

            return ERROR_MAP.getOrDefault(statusCode.getCode(), UnknownException.class);
        }
        return UnknownException.class;
    }

    public static class Builder extends AbstractSts.Builder<GcpSts> {
        protected Builder() {
            providerId(GcpConstants.PROVIDER_ID);
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
