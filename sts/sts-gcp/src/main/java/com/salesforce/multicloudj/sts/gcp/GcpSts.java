package com.salesforce.multicloudj.sts.gcp;

import com.google.auto.service.AutoService;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.protobuf.Duration;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.io.IOException;
import java.util.List;

@SuppressWarnings("rawtypes")
@AutoService(AbstractSts.class)
public class GcpSts extends AbstractSts<GcpSts> {
    IamCredentialsClient stsClient;

    public GcpSts(Builder builder) {
        super(builder);
        try {
            stsClient = IamCredentialsClient.create();
        } catch (IOException e) {
            throw new RuntimeException(e);
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
                .addAllScope(List.of("https://www.googleapis.com/auth/iam"))
                .setLifetime(Duration.newBuilder().setSeconds(request.getExpiration()).build())
                .build();

        GenerateAccessTokenResponse response = this.stsClient.generateAccessToken(accessTokenRequest);
        return new StsCredentials(null, null, (response.getAccessToken()));
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider() {
        return null;
    }

    @Override
    protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
        return null;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        return null;
    }

    public static class Builder extends AbstractSts.Builder<GcpSts> {
        protected Builder() {
            providerId("gcp");
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
