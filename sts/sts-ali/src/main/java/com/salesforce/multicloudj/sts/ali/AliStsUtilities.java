package com.salesforce.multicloudj.sts.ali;

import com.aliyuncs.AcsRequest;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.auth.BasicSessionCredentials;
import com.aliyuncs.auth.DefaultCredentialsProvider;
import com.aliyuncs.auth.Signer;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.sts.driver.AbstractStsUtilities;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.SignedAuthRequest;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpRequest;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

@AutoService(AbstractStsUtilities.class)
public class AliStsUtilities extends AbstractStsUtilities<AliStsUtilities> {
    private static final String SIGN_STS_ENDPOINT = "sts.%s.aliyuncs.com";
    private static final String DEFAULT_API_ACTION_NAME = "GetCallerIdentity";
    private static final String DEFAULT_API_VERSION = "2015-04-01";
    private static final String SERVICE_SIGNING_NAME = "sts";

    public AliStsUtilities(Builder builder) {
        super(builder);
    }

    public AliStsUtilities() {
        super(new Builder());
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    protected SignedAuthRequest newCloudNativeAuthSignedRequest(HttpRequest request) {
        BasicSessionCredentials credentials;
        try {
            if (credentialsOverrider != null && credentialsOverrider.getType() == CredentialsType.SESSION) {
                StsCredentials stsCredentials = credentialsOverrider.getSessionCredentials();
                credentials = new BasicSessionCredentials(stsCredentials.getAccessKeyId(),
                        stsCredentials.getAccessKeySecret(), stsCredentials.getSecurityToken());
            } else {
                credentials = (BasicSessionCredentials) new DefaultCredentialsProvider().getCredentials();
            }
        } catch (ClientException e) {
            throw new RuntimeException(e);
        }

        CommonRequest commonRequest = new CommonRequest();
        commonRequest.setSysDomain(String.format(SIGN_STS_ENDPOINT, region));
        commonRequest.setSysProduct(SERVICE_SIGNING_NAME);
        commonRequest.setSysAction(DEFAULT_API_ACTION_NAME);
        commonRequest.setSysVersion(DEFAULT_API_VERSION);
        commonRequest.setHttpContentType(FormatType.FORM);
        commonRequest.setSysMethod(MethodType.GET);
        commonRequest.setSysProtocol(ProtocolType.HTTPS);
        AcsRequest<?> acsRequest = commonRequest.buildRequest();
        Signer signer = Signer.getSigner(credentials);
        com.aliyuncs.http.HttpRequest signedRequest;
        try {
            signedRequest = acsRequest.signRequest(signer, credentials, acsRequest.getSysAcceptFormat(), acsRequest.getSysProductDomain());
        } catch (InvalidKeyException | UnsupportedEncodingException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        // build a copy of the HttpRequest with signed headers
        HttpRequest.Builder builder;
        try {
            HttpRequest.BodyPublisher bodyPublisher = signedRequest.getHttpContent() != null
                    ? HttpRequest.BodyPublishers.ofByteArray(signedRequest.getHttpContent())
                    : HttpRequest.BodyPublishers.noBody();
            builder = HttpRequest.newBuilder(new URI(signedRequest.getSysUrl())).method(signedRequest.getSysMethod().toString(), bodyPublisher);
            HttpRequest.Builder finalBuilder = builder;
            signedRequest.getSysHeaders().forEach((k, v) -> finalBuilder.setHeader(k, String.join(",", v)));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }


        HttpRequest signed = builder.build();
        return new SignedAuthRequest(signed, new StsCredentials(credentials.getAccessKeyId(), credentials.getAccessKeySecret(), credentials.getSessionToken()));
    }

    public static class Builder extends AbstractStsUtilities.Builder<AliStsUtilities> {
        protected Builder() {
            providerId("ali");
        }

        @Override
        public AliStsUtilities build() {
            return new AliStsUtilities(this);
        }
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        String errorCode = null;
        String errMessage = null;
        if (t instanceof ClientException) {
            errorCode = ((ClientException) t).getErrCode();
            errMessage = t.getMessage();
        } else if (t.getCause() instanceof ClientException) {
            errorCode = ((ClientException) t.getCause()).getErrCode();
            errMessage = t.getCause().getMessage();
        }

        // First, check errorCode in ERROR_CODE_MAPPING
        if (StringUtils.isNotBlank(errorCode)) {
            return ERROR_CODE_MAPPING.getOrDefault(errorCode, UnknownException.class);
        }

        // Then, check errMessage in ERROR_MSG_MAPPING
        if (StringUtils.isNotBlank(errMessage)) {
            return ERROR_MSG_MAPPING.getOrDefault(errMessage, UnknownException.class);
        }

        return UnknownException.class;
    }

    // The common error codes as source of truth is here:
    // https://docs.aws.amazon.com/STS/latest/APIReference/CommonErrors.html
    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_CODE_MAPPING = Map.of(
            "SDK.InvalidAccessKeySecret", InvalidArgumentException.class,
            "InvalidSecurityToken.MismatchWithAccessKey", InvalidArgumentException.class,
            "InvalidSecurityToken.Malformed", InvalidArgumentException.class
            // Add more mappings as needed
    );

    // In alibaba, we observed sometimes the standard err code is null but message is set.
    // Therefore, we need to check both to wrap the exception.
    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MSG_MAPPING = Map.of(
            "not found credentials", UnAuthorizedException.class
            // Add more mappings as needed
    );
}
