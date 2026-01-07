package com.salesforce.multicloudj.sts.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auth.oauth2.ImpersonatedCredentials;
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
import com.salesforce.multicloudj.sts.model.CredentialScope;
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
        // If credential scope is provided, use DownscopedCredentials
        if (request.getCredentialScope() != null) {
            try {
                // Create credentials for the service account
                GoogleCredentials sourceCredentials = getCredentials();

                // If service account impersonation is needed, use ImpersonatedCredentials
                if (request.getRole() != null && !request.getRole().isEmpty()) {
                    ImpersonatedCredentials.Builder impersonatedBuilder = ImpersonatedCredentials.newBuilder()
                            .setSourceCredentials(sourceCredentials)
                            .setTargetPrincipal(request.getRole())
                            .setScopes(List.of(scope));

                    if (request.getExpiration() > 0) {
                        impersonatedBuilder.setLifetime(request.getExpiration());
                    }

                    sourceCredentials = impersonatedBuilder.build();
                }

                // Convert cloud-agnostic CredentialScope to GCP CredentialAccessBoundary
                CredentialAccessBoundary gcpAccessBoundary = convertToGcpAccessBoundary(request.getCredentialScope());

                // Create downscoped credentials with the access boundary
                DownscopedCredentials downscopedCredentials = DownscopedCredentials.newBuilder()
                        .setSourceCredential(sourceCredentials)
                        .setCredentialAccessBoundary(gcpAccessBoundary)
                        .build();

                // Get the downscoped access token
                AccessToken accessToken = downscopedCredentials.refreshAccessToken();
                return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, accessToken.getTokenValue());
            } catch (IOException e) {
                throw new SubstrateSdkException("Failed to create downscoped credentials", e);
            }
        }

        // Original behavior when no access boundary is provided
        GenerateAccessTokenRequest.Builder accessTokenRequestBuilder = GenerateAccessTokenRequest.newBuilder()
                .setName("projects/-/serviceAccounts/" + request.getRole())
                .addAllScope(List.of(scope));
        if (request.getExpiration() > 0) {
            accessTokenRequestBuilder.setLifetime(Duration.newBuilder().setSeconds(request.getExpiration()));
        }
        GenerateAccessTokenResponse response = this.stsClient.generateAccessToken(accessTokenRequestBuilder.build());
        return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, response.getAccessToken());
    }

    /**
     * Converts cloud-agnostic CredentialScope to GCP-specific CredentialAccessBoundary.
     * Maps cloud-agnostic storage actions and resources to GCP format.
     */
    private CredentialAccessBoundary convertToGcpAccessBoundary(
            com.salesforce.multicloudj.sts.model.CredentialScope credentialScope) {
        CredentialAccessBoundary.Builder gcpBoundaryBuilder = CredentialAccessBoundary.newBuilder();

        for (CredentialScope.ScopeRule rule : credentialScope.getRules()) {
            CredentialAccessBoundary.AccessBoundaryRule.Builder gcpRuleBuilder =
                    CredentialAccessBoundary.AccessBoundaryRule.newBuilder()
                            .setAvailableResource(convertToGcpResource(rule.getAvailableResource()));

            // Add permissions - convert cloud-agnostic to GCP format
            for (String permission : rule.getAvailablePermissions()) {
                gcpRuleBuilder.addAvailablePermission(convertToGcpPermission(permission));
            }

            // Add availability condition if present
            if (rule.getAvailabilityCondition() != null) {
                CredentialScope.AvailabilityCondition condition =
                        rule.getAvailabilityCondition();
                CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.Builder gcpConditionBuilder =
                        CredentialAccessBoundary.AccessBoundaryRule.AvailabilityCondition.newBuilder();

                if (condition.getExpression() != null) {
                    // Convert cloud-agnostic expression to GCP CEL format
                    String gcpExpression = convertToGcpExpression(condition.getExpression());
                    gcpConditionBuilder.setExpression(gcpExpression);
                }
                if (condition.getTitle() != null) {
                    gcpConditionBuilder.setTitle(condition.getTitle());
                }
                if (condition.getDescription() != null) {
                    gcpConditionBuilder.setDescription(condition.getDescription());
                }

                gcpRuleBuilder.setAvailabilityCondition(gcpConditionBuilder.build());
            }

            gcpBoundaryBuilder.addRule(gcpRuleBuilder.build());
        }

        return gcpBoundaryBuilder.build();
    }

    /**
     * Converts cloud-agnostic permission to GCP permission format.
     * Example: "storage:GetObject" -> "inRole:roles/storage.objectViewer"
     */
    private String convertToGcpPermission(String permission) {
        // Handle cloud-agnostic storage: format
        // based on the need, this can be extended to more services
        if (permission.startsWith("storage:")) {
            String action = permission.substring("storage:".length());

            // Map common actions to GCP roles
            switch (action) {
                case "GetObject":
                    return "inRole:roles/storage.objectViewer";
                case "PutObject":
                    return "inRole:roles/storage.objectCreator";
                case "DeleteObject":
                    return "inRole:roles/storage.objectAdmin";
                case "ListBucket":
                    return "inRole:roles/storage.objectViewer";
                default:
                    // For unknown actions, default to objectViewer
                    return "inRole:roles/storage.objectViewer";
            }
        }

        // If it's already a GCP format (inRole:*), return as-is
        if (permission.startsWith("inRole:")) {
            return permission;
        }

        // Default: wrap in inRole if no format detected
        return "inRole:" + permission;
    }

    /**
     * Converts cloud-agnostic resource to GCP resource format.
     * Example: "storage://my-bucket/*" -> "//storage.googleapis.com/projects/_/buckets/my-bucket"
     */
    private String convertToGcpResource(String resource) {
        // Handle cloud-agnostic storage:// format
        if (resource.startsWith("storage://")) {
            String path = resource.substring("storage://".length());
            // Remove trailing /* if present
            if (path.endsWith("/*")) {
                path = path.substring(0, path.length() - 2);
            }
            // Extract bucket name (before first /)
            String bucketName = path.contains("/") ? path.substring(0, path.indexOf("/")) : path;
            return "//storage.googleapis.com/projects/_/buckets/" + bucketName;
        }

        // If it's already a GCP format (//storage.googleapis.com/*), return as-is
        if (resource.startsWith("//storage.googleapis.com/")) {
            return resource;
        }

        // Default: assume it's a bucket name
        return "//storage.googleapis.com/projects/_/buckets/" + resource;
    }

    /**
     * Converts cloud-agnostic CEL expression to GCP CEL format.
     * Example: "resource.name.startsWith('storage://my-bucket/prefix/')" ->
     *          "resource.name.startsWith('projects/_/buckets/my-bucket/objects/prefix/')"
     */
    private String convertToGcpExpression(String expression) {
        // Replace storage:// with GCP format in expressions
        if (expression.contains("storage://")) {
            return expression.replace("storage://", "projects/_/buckets/")
                           .replace("/", "/objects/");
        }
        return expression;
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
        throw new UnSupportedOperationException("Not supported yet.");
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
            if (System.getenv("KUBERNETES_SERVICE_HOST") != null) {
                return ComputeEngineCredentials.create();
            }
            GoogleCredentials adc = GoogleCredentials.getApplicationDefault();
            if (adc.createScopedRequired()) {
                adc = adc.createScoped(List.of(scope));
            }
            return adc;
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
