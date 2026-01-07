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
import com.salesforce.multicloudj.sts.model.CredentialScope;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
        AssumeRoleRequest.Builder roleRequestBuilder = AssumeRoleRequest.builder()
                .roleArn(request.getRole())
                .roleSessionName(request.getSessionName() != null ? request.getSessionName() : "multicloudj-" + System.currentTimeMillis())
                .durationSeconds(request.getExpiration() != 0 ? request.getExpiration() : null);

        // If credential scope is provided, convert to AWS IAM policy JSON
        if (request.getCredentialScope() != null) {
            String policyJson = convertToAwsPolicy(request.getCredentialScope());
            roleRequestBuilder.policy(policyJson);
        }

        AssumeRoleResponse response = stsClient.assumeRole(roleRequestBuilder.build());
        Credentials credentials = response.credentials();

        return new StsCredentials(
                credentials.accessKeyId(),
                credentials.secretAccessKey(),
                credentials.sessionToken());
    }

    /**
     * Converts cloud-agnostic CredentialScope to AWS IAM Policy JSON.
     */
    private String convertToAwsPolicy(CredentialScope credentialScope) {
        List<Map<String, Object>> statements = new ArrayList<>();

        for (CredentialScope.ScopeRule rule : credentialScope.getRules()) {
            Map<String, Object> statement = new HashMap<>();
            statement.put("Effect", "Allow");

            // Convert permissions (format: "inRole:roles/storage.objectViewer" -> "Action")
            List<String> actions = rule.getAvailablePermissions().stream()
                    .map(this::convertPermissionToAction)
                    .collect(Collectors.toList());
            statement.put("Action", actions);

            // Convert resource (format: "//storage.googleapis.com/..." -> "arn:aws:...")
            String resource = convertResourceToArn(rule.getAvailableResource());
            statement.put("Resource", resource);

            // Add condition if present
            if (rule.getAvailabilityCondition() != null) {
                Map<String, Object> condition = convertConditionToAwsCondition(
                        rule.getAvailabilityCondition());
                if (!condition.isEmpty()) {
                    statement.put("Condition", condition);
                }
            }

            statements.add(statement);
        }

        Map<String, Object> policy = new HashMap<>();
        policy.put("Version", "2012-10-17");
        policy.put("Statement", statements);

        // Convert to JSON string
        return toJsonString(policy);
    }

    /**
     * Converts cloud-agnostic permission to AWS Action.
     * Maps MultiCloudJ storage actions to AWS S3 actions.
     * Example: "storage:GetObject" -> "s3:GetObject"
     */
    private String convertPermissionToAction(String permission) {
        // Handle cloud-agnostic storage: format
        if (permission.startsWith("storage:")) {
            String action = permission.substring("storage:".length());
            return "s3:" + action;  // storage:GetObject -> s3:GetObject
        }

        // If it's already an AWS action format (s3:*, iam:*, etc.), return as-is
        if (permission.contains(":")) {
            return permission;
        }

        // Default: assume s3 if no service specified
        return "s3:" + permission;
    }

    /**
     * Converts cloud-agnostic resource to AWS ARN.
     * Maps MultiCloudJ storage URIs to AWS S3 ARNs.
     * Example: "storage://my-bucket/*" -> "arn:aws:s3:::my-bucket/*"
     */
    private String convertResourceToArn(String resource) {
        // Handle cloud-agnostic storage:// format
        if (resource.startsWith("storage://")) {
            String path = resource.substring("storage://".length());
            return "arn:aws:s3:::" + path;  // storage://my-bucket/* -> arn:aws:s3:::my-bucket/*
        }

        // If it's already an AWS ARN format (arn:aws:*), return as-is
        if (resource.startsWith("arn:aws:")) {
            return resource;
        }

        // Default: assume S3 bucket path
        return "arn:aws:s3:::" + resource;
    }

    /**
     * Converts cloud-agnostic availability condition to AWS IAM condition.
     * Note: This is a simplified implementation. Full CEL expression parsing
     * would require a more sophisticated approach.
     */
    private Map<String, Object> convertConditionToAwsCondition(
            CredentialScope.AvailabilityCondition condition) {
        Map<String, Object> awsCondition = new HashMap<>();

        if (condition.getExpression() == null || condition.getExpression().isEmpty()) {
            return awsCondition;
        }

        String expression = condition.getExpression();

        // Handle common patterns
        // Example: "resource.name.startsWith('storage://my-bucket/prefix/')"
        if (expression.contains("startsWith") && expression.contains("storage://")) {
            // Extract the prefix from the expression
            int startIdx = expression.indexOf("storage://");
            int endIdx = expression.indexOf("'", startIdx + 1);
            if (endIdx > startIdx) {
                String prefix = expression.substring(startIdx + "storage://".length(), endIdx);
                // Remove bucket name, keep only the path prefix
                if (prefix.contains("/")) {
                    String pathPrefix = prefix.substring(prefix.indexOf("/") + 1);
                    if (!pathPrefix.isEmpty()) {
                        Map<String, String> stringLike = new HashMap<>();
                        stringLike.put("s3:prefix", pathPrefix);
                        awsCondition.put("StringLike", stringLike);
                    }
                }
            }
        }

        return awsCondition;
    }

    /**
     * Converts Map to JSON string.
     */
    private String toJsonString(Map<String, Object> map) {
        // Simple JSON serialization - in production, use a proper JSON library
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (!first) json.append(",");
            first = false;
            json.append("\"").append(entry.getKey()).append("\":");
            json.append(toJsonValue(entry.getValue()));
        }
        json.append("}");
        return json.toString();
    }

    @SuppressWarnings("unchecked")
    private String toJsonValue(Object value) {
        if (value == null) {
            return "null";
        } else if (value instanceof String) {
            return "\"" + value + "\"";
        } else if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            return "[" + list.stream()
                    .map(this::toJsonValue)
                    .collect(Collectors.joining(",")) + "]";
        } else if (value instanceof Map) {
            return toJsonString((Map<String, Object>) value);
        }
        return "\"" + value + "\"";
    }

    @Override
    protected CallerIdentity getCallerIdentityFromProvider(com.salesforce.multicloudj.sts.model.GetCallerIdentityRequest request) {
        GetCallerIdentityRequest callerIdentityRequest = GetCallerIdentityRequest.builder().build();
        GetCallerIdentityResponse response = stsClient.getCallerIdentity(callerIdentityRequest);
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
                .roleSessionName(request.getSessionName() != null ? request.getSessionName() : "multicloudj-web-identity-" + System.currentTimeMillis())
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
