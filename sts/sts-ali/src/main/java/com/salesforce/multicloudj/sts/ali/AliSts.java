package com.salesforce.multicloudj.sts.ali;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.DefaultCredentialsProvider;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleResponse;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityRequest;
import com.aliyuncs.sts.model.v20150401.GetCallerIdentityResponse;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractSts;
import com.salesforce.multicloudj.sts.model.AssumeRoleWebIdentityRequest;
import com.salesforce.multicloudj.sts.model.AssumedRoleRequest;
import com.salesforce.multicloudj.sts.model.CallerIdentity;
import com.salesforce.multicloudj.sts.model.GetAccessTokenRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;

import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AutoService(AbstractSts.class)
public class AliSts extends AbstractSts {

    private IAcsClient stsClient;

    public AliSts(Builder builder) {
        super(builder);

        DefaultProfile clientProfile;
        if (builder.getEndpoint() != null) {
            clientProfile = getClientProfile(builder.getEndpoint());
        } else {
            clientProfile = DefaultProfile.getProfile(builder.getRegion());
        }
        this.stsClient = new DefaultAcsClient(clientProfile);
    }

    // Alibaba STS client doesn't directly support the endpoints override but
    // have the chained endpoint resolver which resolved endpoint based on the
    // IClientProfile.
    static DefaultProfile getClientProfile(URI endpoint) {
        DefaultProfile profile;
        // Regex pattern to match the first part and the region part
        // <product>.<region>.aliyuncs.com
        // product can be sts or sts-vpc
        String regex = "^([^.]+)\\.([^.]+)\\.([^.]+)\\.([^.]+)$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(endpoint.getHost());

        if (matcher.matches()) {
            String product = matcher.group(1);
            String region = matcher.group(2);
            profile = DefaultProfile.getProfile(region);
            if (product.endsWith("-vpc")) {
                profile.enableUsingVpcEndpoint();
            }
        } else {
            throw new InvalidArgumentException("The endpoint is invalid");
        }

        return profile;
    }

    public AliSts(Builder builder, IAcsClient stsClient) {
        super(builder);
        this.stsClient = stsClient;
    }

    public AliSts() {
        super(new Builder());
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request) {
        AssumeRoleRequest roleRequest = new AssumeRoleRequest();
        roleRequest.setRoleArn(request.getRole());
        roleRequest.setRoleSessionName(request.getSessionName());
        if (request.getExpiration() > 0) {
            roleRequest.setDurationSeconds((long) request.getExpiration());
        }

        try {
            AssumeRoleResponse response = stsClient.getAcsResponse(roleRequest);
            AssumeRoleResponse.Credentials credentials = response.getCredentials();
            return new StsCredentials(
                    credentials.getAccessKeyId(),
                    credentials.getAccessKeySecret(),
                    credentials.getSecurityToken());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider(com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest request) {
        GetCallerIdentityRequest callerIdentityRequest = new GetCallerIdentityRequest();
        try {
            GetCallerIdentityResponse response = stsClient.getAcsResponse(callerIdentityRequest);
            return new CallerIdentity(response.getPrincipalId(), response.getArn(), response.getAccountId());
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
    }

    /*
    TODO: The correct implementation of GetAccessToken is to use the acs request GenerateSessionAccessKey
     This is only available to japan region so far.
     https://github.com/aliyun/nodejs-sts-sdk?tab=readme-ov-file#method-japan-only--generatesessionaccesskey
     We should follow up with alibaba if this feature will be available to cn-shanghai.
    */
    @Override
    protected StsCredentials getAccessTokenFromProvider(GetAccessTokenRequest request) {
        try {

            DefaultCredentialsProvider credentialsProvider = new DefaultCredentialsProvider();
            BasicSessionCredentials credentials = (BasicSessionCredentials) credentialsProvider.getCredentials();
            return new StsCredentials(
                    credentials.getAccessKeyId(),
                    credentials.getAccessKeySecret(),
                    credentials.getSessionToken());
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRoleWebIdentity(AssumeRoleWebIdentityRequest request) {
        throw new UnSupportedOperationException("Not supported yet.");
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        // ali-yun has a wierd chain where ServerException extends the ClientException
        if (t instanceof ClientException) {
            String errorCode = ((ClientException) t).getErrCode();
            return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
        }
        return UnknownException.class;
    }

    // The common error codes as source of truth is here:
    // https://docs.ali.amazon.com/STS/latest/APIReference/CommonErrors.html
    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING = Map.of(
            "SDK.InvalidAccessKeySecret", InvalidArgumentException.class,
            "InvalidSecurityToken.MismatchWithAccessKey", InvalidArgumentException.class,
            "InvalidSecurityToken.Malformed", InvalidArgumentException.class,
            "InvalidAccessKeyId.NotFound", InvalidArgumentException.class,
            "SignatureNotMatch", UnAuthorizedException.class,
            "Unauthorized", UnAuthorizedException.class,
            "InternalServerError", UnknownException.class,
            "ServerBusy", UnknownException.class,
            "MissingRoleSessionName", InvalidArgumentException.class
            // Add more mappings as needed
    );

    public static class Builder extends AbstractSts.Builder<AliSts, Builder> {
        protected Builder() {
            providerId("ali");
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public AliSts build() {
            return new AliSts(this);
        }

        public AliSts build(DefaultAcsClient stsClient) {
            return new AliSts(this, stsClient);
        }
    }
}
