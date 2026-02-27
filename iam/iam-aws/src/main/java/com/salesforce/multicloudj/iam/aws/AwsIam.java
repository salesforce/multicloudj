package com.salesforce.multicloudj.iam.aws;

import com.google.auto.service.AutoService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.multicloudj.common.aws.AwsConstants;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.provider.Provider;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.CreateIdentityRequest;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.DeleteIdentityRequest;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetIdentityRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.RemovePolicyRequest;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.IamClientBuilder;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.EntityAlreadyExistsException;
import software.amazon.awssdk.services.iam.model.GetRoleRequest;
import software.amazon.awssdk.services.iam.model.GetRoleResponse;
import software.amazon.awssdk.services.iam.model.GetRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.GetRolePolicyResponse;
import software.amazon.awssdk.services.iam.model.Role;
import software.amazon.awssdk.services.iam.model.UpdateAssumeRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.UpdateRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.PutRolePolicyRequest;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@AutoService(AbstractIam.class)
public class AwsIam extends AbstractIam {

    public static final String POLICY_VERSION = "2012-10-17";
    public static final String ALLOW = "Allow";
    public static final String STS_ASSUME_ROLE = "sts:AssumeRole";
    public static final String ARN_PREFIX = "arn:";
    public static final String AWS_ACCOUNT_ID_REGEX = "\\d{12}";
    public static final String ARN_AWS_IAM_PREFIX = "arn:aws:iam::";
    public static final String ROOT_SUFFIX = ":root";
    public static final String SERVICE_PRINCIPAL_SUFFIX = ".amazonaws.com";
    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private IamClient iamClient;

    public AwsIam(Builder builder) {
        super(builder);
        this.iamClient = builder.iamClient;
    }

    public AwsIam() {
        this(Builder.defaultBuilder());
    }

    public static class Builder extends AbstractIam.Builder<AwsIam, Builder> {

        private IamClient iamClient;

        public Builder() {
            providerId(AwsConstants.PROVIDER_ID);
        }

        public Builder withIamClient(IamClient iamClient) {
            this.iamClient = iamClient;
            return this;
        }

        private static Builder defaultBuilder() {
            Builder builder = new Builder();
            builder.iamClient = buildIamClient(builder);
            return builder;
        }

        private static IamClient buildIamClient(Builder builder) {
            // NOTE: AWS IAM is a global service per AWS Partition.
            String regionStr = builder.getRegion() != null ? builder.getRegion() : "us-east-1";
            Region regionObj = Region.of(regionStr);
            IamClientBuilder clientBuilder = IamClient.builder().region(regionObj);

            AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), regionObj);
            if (credentialsProvider != null) {
                clientBuilder.credentialsProvider(credentialsProvider);
            }
            if (builder.getEndpoint() != null) {
                clientBuilder.endpointOverride(builder.getEndpoint());
            }
            return clientBuilder.build();
        }

        @Override
        public Builder self() {
            return this;
        }

        @Override
        public AwsIam build() {
            if (this.iamClient == null) {
                this.iamClient = buildIamClient(this);
            }
            return new AwsIam(this);
        }
    }

    @Override
    public Provider.Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof SubstrateSdkException && !t.getClass().equals(SubstrateSdkException.class)) {
            return (Class<? extends SubstrateSdkException>) t.getClass();
        }
        if (t instanceof AwsServiceException) {
            AwsServiceException serviceException = (AwsServiceException) t;
            if (serviceException.awsErrorDetails() != null) {
                String errorCode = serviceException.awsErrorDetails().errorCode();
                return ErrorCodeMapping.getException(errorCode);
            }
            return UnknownException.class;
        }
        if (t instanceof SdkClientException || t instanceof IllegalArgumentException) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    @Override
    public void close() throws Exception {
        if (this.iamClient != null) {
            this.iamClient.close();
        }
    }

    /**
     * Create IAM Role with optional Trust Configuration and Create Options.
     *
     * @param request the request containing identity name, description, tenant ID, region, trust config, and options.
     * @return the IAM role ARN.
     */
    @Override
    protected String doCreateIdentity(CreateIdentityRequest request) {
        String assumeRolePolicyDocument = buildAssumeRolePolicyDocument(request.getTenantId(), request.getTrustConfig());

        CreateRoleRequest.Builder requestBuilder = CreateRoleRequest.builder()
                .roleName(request.getIdentityName())
                .assumeRolePolicyDocument(assumeRolePolicyDocument)
                .description(StringUtils.defaultString(request.getDescription()));

        if (request.getOptions().isPresent()) {
            CreateOptions opts = request.getOptions().get();
            if (StringUtils.isNotBlank(opts.getPath())) {
                requestBuilder.path(opts.getPath());
            }
            if (opts.getMaxSessionDuration() != null) {
                requestBuilder.maxSessionDuration(opts.getMaxSessionDuration());
            }
            if (StringUtils.isNotBlank(opts.getPermissionBoundary())) {
                requestBuilder.permissionsBoundary(opts.getPermissionBoundary());
            }
        }

        try {
            CreateRoleResponse response = this.iamClient.createRole(requestBuilder.build());
            Role role = response.role();
            return role != null ? role.arn() : null;
        } catch (EntityAlreadyExistsException e) {
            GetRoleResponse getRoleResponse = this.iamClient.getRole(GetRoleRequest.builder().roleName(request.getIdentityName()).build());
            Role existingRole = getRoleResponse.role();
            
            if (existingRole != null) {
                updateRoleIfNeeded(existingRole, request.getDescription(), assumeRolePolicyDocument, request.getOptions());
            }
            
            return existingRole != null ? existingRole.arn() : null;
        }
    }

    /**
     * Updates the role if any of the attributes (description, trust policy, or create options) have changed.
     *
     * @param existingRole the existing role
     * @param newDescription the new description
     * @param newAssumeRolePolicyDocument the new assume role policy document
     * @param options the create options
     */
    private void updateRoleIfNeeded(Role existingRole, String newDescription,
                                     String newAssumeRolePolicyDocument, Optional<CreateOptions> options) {
        boolean needsUpdate = false;
        
        String existingDescription = StringUtils.defaultString(existingRole.description());
        String targetDescription = StringUtils.defaultString(newDescription);
        
        if (!existingDescription.equals(targetDescription)) {
            needsUpdate = true;
        }
        
        if (!needsUpdate && options.isPresent()) {
            CreateOptions opts = options.get();
            if (opts.getMaxSessionDuration() != null && 
                !opts.getMaxSessionDuration().equals(existingRole.maxSessionDuration())) {
                needsUpdate = true;
            }
        }
        
        if (needsUpdate) {
            UpdateRoleRequest.Builder updateBuilder = UpdateRoleRequest.builder()
                    .roleName(existingRole.roleName())
                    .description(targetDescription);
            
            if (options.isPresent() && options.get().getMaxSessionDuration() != null) {
                updateBuilder.maxSessionDuration(options.get().getMaxSessionDuration());
            }
            
            this.iamClient.updateRole(updateBuilder.build());
        }
        
        if (isTrustPolicyDifferent(existingRole.assumeRolePolicyDocument(), newAssumeRolePolicyDocument)) {
            UpdateAssumeRolePolicyRequest updatePolicyRequest = UpdateAssumeRolePolicyRequest.builder()
                    .roleName(existingRole.roleName())
                    .policyDocument(newAssumeRolePolicyDocument)
                    .build();
            this.iamClient.updateAssumeRolePolicy(updatePolicyRequest);
        }
    }
    
    /**
     * Compares two assume role policy documents to determine if they are different.
     * The existing policy is URL-encoded, so we decode it before comparison.
     *
     * @param existingPolicyEncoded the existing policy document (URL-encoded)
     * @param newPolicy the new policy document
     * @return true if the policies are different, false otherwise
     */
    private boolean isTrustPolicyDifferent(String existingPolicyEncoded, String newPolicy) {
        if (existingPolicyEncoded == null && newPolicy == null) {
            return false;
        }
        if (existingPolicyEncoded == null || newPolicy == null) {
            return true;
        }
        
        try {
            String existingPolicy = URLDecoder.decode(existingPolicyEncoded, StandardCharsets.UTF_8);
            
            JsonNode existingJson = OBJECT_MAPPER.readTree(existingPolicy);
            JsonNode newJson = OBJECT_MAPPER.readTree(newPolicy);
            
            return !existingJson.equals(newJson);
        } catch (Exception e) {
            return !existingPolicyEncoded.equals(newPolicy);
        }
    }

    private String buildAssumeRolePolicyDocument(String tenantId, Optional<TrustConfiguration> trustConfig) {
        List<String> awsPrincipals = new ArrayList<>();
        List<String> servicePrincipals = new ArrayList<>();

        if (trustConfig.isPresent() && trustConfig.get().getTrustedPrincipals() != null
                && !trustConfig.get().getTrustedPrincipals().isEmpty()) {
            for (String trustedPrincipal : trustConfig.get().getTrustedPrincipals()) {
                if (StringUtils.isBlank(trustedPrincipal)) {
                    continue;
                }

                if (trustedPrincipal.startsWith(ARN_PREFIX)) {
                    awsPrincipals.add(trustedPrincipal);
                } else if (trustedPrincipal.matches(AWS_ACCOUNT_ID_REGEX)) {
                    awsPrincipals.add(ARN_AWS_IAM_PREFIX + trustedPrincipal + ROOT_SUFFIX);
                } else if (trustedPrincipal.endsWith(SERVICE_PRINCIPAL_SUFFIX)) {
                    servicePrincipals.add(trustedPrincipal);
                } else {
                    awsPrincipals.add(trustedPrincipal);
                }
            }
        }

        if (awsPrincipals.isEmpty() && servicePrincipals.isEmpty()) {
            awsPrincipals.add(ARN_AWS_IAM_PREFIX + tenantId + ROOT_SUFFIX);
        }

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("Version", POLICY_VERSION);

        Map<String, Object> stmt = new LinkedHashMap<>();
        stmt.put("Effect", ALLOW);
        stmt.put("Action", STS_ASSUME_ROLE);

        Map<String, Object> principal = new LinkedHashMap<>();
        if (!awsPrincipals.isEmpty()) {
            principal.put("AWS", awsPrincipals);
        }
        if (!servicePrincipals.isEmpty()) {
            principal.put("Service", servicePrincipals);
        }
        stmt.put("Principal", principal);

        if (trustConfig.isPresent() && trustConfig.get().getConditions() != null && !trustConfig.get().getConditions().isEmpty()) {
            stmt.put("Condition", trustConfig.get().getConditions());
        }

        doc.put("Statement", List.of(stmt));
        try {
            return OBJECT_MAPPER.writeValueAsString(doc);
        } catch (JsonProcessingException e) {
            throw new SubstrateSdkException("Failed to serialize assume role policy document", e);
        }
    }

    @Override
    protected void doAttachInlinePolicy(AttachInlinePolicyRequest request) {
        if (StringUtils.isBlank(request.getPolicyDocument().getName())) {
            throw new InvalidArgumentException("policy name is required for AWS IAM");
        }

        String roleName = request.getIdentityName();
        String policyDocumentJson = buildInlinePolicyDocumentJson(request.getPolicyDocument());

        PutRolePolicyRequest awsRequest = PutRolePolicyRequest.builder()
                .roleName(roleName)
                .policyName(request.getPolicyDocument().getName())
                .policyDocument(policyDocumentJson)
                .build();

        this.iamClient.putRolePolicy(awsRequest);
    }

    private static String buildInlinePolicyDocumentJson(PolicyDocument policyDocument) {
        String version = policyDocument.getVersion();
        if (StringUtils.isBlank(version)) {
            version = POLICY_VERSION;
        }
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("Version", version);

        List<Map<String, Object>> awsStatements = new ArrayList<>();
        for (Statement stmt : policyDocument.getStatements()) {
            Map<String, Object> awsStmt = new LinkedHashMap<>();
            awsStmt.put("Effect", stmt.getEffect());

            List<String> actions = stmt.getActions();
            if (actions != null && !actions.isEmpty()) {
                awsStmt.put("Action", actions);
            }
            if (StringUtils.isNotBlank(stmt.getSid())) {
                awsStmt.put("Sid", stmt.getSid());
            }
            if (stmt.getResources() != null && !stmt.getResources().isEmpty()) {
                awsStmt.put("Resource", stmt.getResources());
            }
            if (stmt.getConditions() != null && !stmt.getConditions().isEmpty()) {
                awsStmt.put("Condition", stmt.getConditions());
            }
            if (stmt.getPrincipals() != null && !stmt.getPrincipals().isEmpty()) {
                awsStmt.put("Principal", stmt.getPrincipals());
            }

            awsStatements.add(awsStmt);
        }
        doc.put("Statement", awsStatements);

        try {
            return OBJECT_MAPPER.writeValueAsString(doc);
        } catch (JsonProcessingException e) {
            throw new InvalidArgumentException("Failed to serialize inline policy document", e);
        }
    }

    /**
     * Get inline policy document attached to an IAM role.
     * @param request the request containing relevant fields from identity name, policy name, role name, tenant ID, and region
     * @return the inline policy document as a JSON string
     */
    @Override
    protected String doGetInlinePolicyDetails(GetInlinePolicyDetailsRequest request) {
        if (StringUtils.isBlank(request.getRoleName())) {
            throw new InvalidArgumentException("roleName is required for AWS IAM");
        }
        if (StringUtils.isBlank(request.getPolicyName())) {
            throw new InvalidArgumentException("policyName is required for AWS IAM");
        }

        IamClient client = this.iamClient;
        GetRolePolicyRequest awsRequest = GetRolePolicyRequest.builder()
                .roleName(request.getRoleName())
                .policyName(request.getPolicyName())
                .build();
        GetRolePolicyResponse response = client.getRolePolicy(awsRequest);
        return URLDecoder.decode(response.policyDocument(), StandardCharsets.UTF_8);
    }

    /**
     * Lists all inline policies attached to an IAM role.
     *
     * @param request the request; AWS uses roleName only (IAM role to list policies for)
     * @return a list of inline policy names attached to the role.
     */
    @Override
    protected List<String> doGetAttachedPolicies(GetAttachedPoliciesRequest request) {
        if (StringUtils.isBlank(request.getRoleName())) {
            throw new InvalidArgumentException("roleName is required for AWS IAM");
        }
        return iamClient.listRolePoliciesPaginator(req -> req.roleName(request.getRoleName()))
                .policyNames()
                .stream()
                .collect(Collectors.toList());
    }

    /**
     * Removes an inline policy from an IAM role.
     *
     * @param request the request containing identity name, policy name, tenant ID, and region.
     */
    @Override
    protected void doRemovePolicy(RemovePolicyRequest request) {
        DeleteRolePolicyRequest deleteRequest = DeleteRolePolicyRequest.builder()
                .roleName(request.getIdentityName())
                .policyName(request.getPolicyName())
                .build();
        
        this.iamClient.deleteRolePolicy(deleteRequest);
    }


    /**
     * Delete IAM Role.
     *
     * @param request the request containing identity name, tenant ID, and region.
     */
    @Override
    protected void doDeleteIdentity(DeleteIdentityRequest request) {
        DeleteRoleRequest deleteRoleRequest = DeleteRoleRequest.builder()
                .roleName(request.getIdentityName())
                .build();
        this.iamClient.deleteRole(deleteRoleRequest);
    }

    /**
     * Get IAM Role.
     *
     * @param request the request containing identity name, tenant ID, and region.
     * @return the IAM role ARN.
     */
    @Override
    protected String doGetIdentity(GetIdentityRequest request) {
        GetRoleRequest getRoleRequest = GetRoleRequest.builder()
                .roleName(request.getIdentityName())
                .build();
        GetRoleResponse response = this.iamClient.getRole(getRoleRequest);
        Role role = response.role();
        return role != null ? role.arn() : null;
    }
}
