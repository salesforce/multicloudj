package com.salesforce.multicloudj.sts.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.CredentialAccessBoundary;
import com.google.auth.oauth2.DownscopedCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auto.service.AutoService;
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
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AutoService(AbstractSts.class)
public class GcpSts extends AbstractSts {
    private static final String STS_ENDPOINT = "https://sts.googleapis.com/v1/token";
    private static final String SCOPE = "https://www.googleapis.com/auth/cloud-platform";

    private GoogleCredentials googleCredentials;
    private HttpTransportFactory httpTransportFactory;

    public GcpSts(Builder builder) {
        super(builder);
    }

    public GcpSts(Builder builder, GoogleCredentials credentials) {
        super(builder);
        this.googleCredentials = credentials;
    }

    public GcpSts(Builder builder, HttpTransportFactory httpTransportFactory) {
        super(builder);
        this.httpTransportFactory = httpTransportFactory;
    }

    public GcpSts(Builder builder, GoogleCredentials credentials,
                  HttpTransportFactory httpTransportFactory) {
        super(builder);
        this.googleCredentials = credentials;
        this.httpTransportFactory = httpTransportFactory;
    }

    public GcpSts() {
        super(new Builder());
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

                // Convert cloud-agnostic resourcePrefix to GCP CEL format
                if (condition.getResourcePrefix() != null) {
                    String gcpExpression = buildGcpPrefixExpression(condition.getResourcePrefix());
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
     * For now, it's limited to storage/gcs service.
     * Example: "storage:GetObject" -> "inRole:roles/storage.objectViewer"
     */
    private String convertToGcpPermission(String permission) {
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

    /**
     * Converts cloud-agnostic resource to GCP resource format.
     * For now, it's limited to storage/gcs service.
     * Example: "storage://my-bucket" -> "//storage.googleapis.com/projects/_/buckets/my-bucket"
     */
    private String convertToGcpResource(String resource) {
        String bucketName = resource.substring("storage://".length());
        return "//storage.googleapis.com/projects/_/buckets/" + bucketName;
    }

    /**
     * Builds GCP CEL expression from cloud-agnostic resource prefix.
     * Example: "storage://my-bucket/documents/" ->
     *          "resource.name.startsWith('projects/_/buckets/my-bucket/objects/documents/')"
     */
    private String buildGcpPrefixExpression(String resourcePrefix) {
        String path = resourcePrefix.substring("storage://".length());
        // Extract bucket name (before first /)
        int slashIdx = path.indexOf('/');
        String bucketName = path.substring(0, slashIdx);
        String prefix = path.substring(slashIdx + 1);
        String gcpPath = "projects/_/buckets/" + bucketName + "/objects/" + prefix;
        return "resource.name.startsWith('" + gcpPath + "')";
    }

    @Override
    protected StsCredentials getSTSCredentialsWithAssumeRole(AssumedRoleRequest request){
        try {
            // Create credentials for the service account
            GoogleCredentials sourceCredentials = getCredentials();

            // If service account impersonation is needed, use ImpersonatedCredentials
            if (request.getRole() != null && !request.getRole().isEmpty()) {
                ImpersonatedCredentials.Builder impersonatedBuilder = ImpersonatedCredentials.newBuilder()
                        .setSourceCredentials(sourceCredentials)
                        .setTargetPrincipal(request.getRole())
                        .setScopes(List.of(SCOPE));

                if (request.getExpiration() > 0) {
                    impersonatedBuilder.setLifetime(request.getExpiration());
                }

                // Set custom HTTP transport if available
                if (httpTransportFactory != null) {
                    impersonatedBuilder.setHttpTransportFactory(httpTransportFactory);
                }

                sourceCredentials = impersonatedBuilder.build();
            }

            // If credential scope is provided, apply downscoping
            if (request.getCredentialScope() != null) {
                // Convert cloud-agnostic CredentialScope to GCP CredentialAccessBoundary
                CredentialAccessBoundary gcpAccessBoundary = convertToGcpAccessBoundary(request.getCredentialScope());

                // Create downscoped credentials with the access boundary
                DownscopedCredentials.Builder downscopedBuilder = DownscopedCredentials.newBuilder()
                        .setSourceCredential(sourceCredentials)
                        .setCredentialAccessBoundary(gcpAccessBoundary);
                DownscopedCredentials downscopedCredentials = downscopedBuilder.build();

                // Get the downscoped access token
                downscopedCredentials.refreshIfExpired();
                AccessToken accessToken = downscopedCredentials.getAccessToken();
                return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, accessToken.getTokenValue());
            }

            // No downscoping - refresh and return the credentials
            sourceCredentials.refreshIfExpired();
            AccessToken accessToken = sourceCredentials.getAccessToken();
            return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, accessToken.getTokenValue());
        } catch (IOException e) {
            throw new SubstrateSdkException("Failed to create credentials", e);
        }
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
        if (request == null) {
            throw new InvalidArgumentException("request cannot be null");
        }
        if (StringUtils.isBlank(request.getRole())) {
            throw new InvalidArgumentException("role (identity pool provider) is required for gcp token exchange");
        }
        if (StringUtils.isBlank(request.getWebIdentityToken())) {
            throw new InvalidArgumentException("webIdentityToken is required for gcp token exchange");
        }

        try {
            // Build token exchange request
            GenericJson tokenRequest = new GenericJson();
            tokenRequest.set("audience", request.getRole());
            tokenRequest.set("grantType", "urn:ietf:params:oauth:grant-type:token-exchange");
            tokenRequest.set("requestedTokenType", "urn:ietf:params:oauth:token-type:access_token");
            tokenRequest.set("subjectToken", request.getWebIdentityToken());
            tokenRequest.set("subjectTokenType", "urn:ietf:params:oauth:token-type:jwt");
            tokenRequest.set("scope", SCOPE);

            // Execute token exchange
            HttpTransport transport = httpTransportFactory != null ? httpTransportFactory.create() : new NetHttpTransport();
            JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
            HttpRequest httpRequest = transport.createRequestFactory()
                    .buildPostRequest(new GenericUrl(STS_ENDPOINT),
                            new ByteArrayContent("application/json", jsonFactory.toByteArray(tokenRequest)));
            com.google.api.client.http.HttpResponse response = httpRequest.execute();
            GenericJson responseData = jsonFactory.fromInputStream(
                    response.getContent(),
                    response.getContentCharset(),
                    GenericJson.class);
            String accessToken = String.valueOf(responseData.get("access_token"));

            return new StsCredentials(StringUtils.EMPTY, StringUtils.EMPTY, accessToken);
        } catch (IOException e) {
            throw new SubstrateSdkException("Failed to exchange OIDC token for GCP access token", e);
        }
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
                adc = adc.createScoped(List.of(SCOPE));
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

        public GcpSts build(GoogleCredentials credentials) {
            return new GcpSts(this, credentials);
        }

        public GcpSts build(HttpTransportFactory httpTransportFactory) {
            return new GcpSts(this, httpTransportFactory);
        }

        public GcpSts build(GoogleCredentials credentials, HttpTransportFactory httpTransportFactory) {
            return new GcpSts(this, credentials, httpTransportFactory);
        }

        @Override
        public GcpSts build() {
            return new GcpSts(this);
        }
    }
}
