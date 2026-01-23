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
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
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

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
     * @param identityName the IAM role name (e.g., "MyApplicationRole").
     * @param description optional description for the role.
     * @param tenantId the AWS Account ID.
     * @param region the AWS region for the IAM client.
     * @param trustConfig optional trust configuration.
     * @param options optional creation options for the role.
     * @return the IAM role ARN.
     */
    @Override
    protected String doCreateIdentity(String identityName, String description, String tenantId, String region, Optional<TrustConfiguration> trustConfig, Optional<CreateOptions> options) {
        String assumeRolePolicyDocument = buildAssumeRolePolicyDocument(tenantId, trustConfig);

        CreateRoleRequest.Builder requestBuilder = CreateRoleRequest.builder()
                .roleName(identityName)
                .assumeRolePolicyDocument(assumeRolePolicyDocument)
                .description(description != null ? description : StringUtils.EMPTY);

        if (options.isPresent()) {
            CreateOptions opts = options.get();
            if (opts.getPath() != null && !opts.getPath().isBlank()) {
                requestBuilder.path(opts.getPath());
            }
            if (opts.getMaxSessionDuration() != null) {
                requestBuilder.maxSessionDuration(opts.getMaxSessionDuration());
            }
            if (opts.getPermissionBoundary() != null && !opts.getPermissionBoundary().isBlank()) {
                requestBuilder.permissionsBoundary(opts.getPermissionBoundary());
            }
        }

        try {
            CreateRoleResponse response = this.iamClient.createRole(requestBuilder.build());
            Role role = response.role();
            return role != null ? role.arn() : null;
        } catch (EntityAlreadyExistsException e) {
            GetRoleResponse getRoleResponse = this.iamClient.getRole(GetRoleRequest.builder().roleName(identityName).build());
            Role existingRole = getRoleResponse.role();
            
            if (existingRole != null) {
                updateRoleIfNeeded(existingRole, description, assumeRolePolicyDocument, options);
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
        
        String existingDescription = existingRole.description() != null ? existingRole.description() : StringUtils.EMPTY;
        String targetDescription = newDescription != null ? newDescription : StringUtils.EMPTY;
        
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
            principal.put("AWS", awsPrincipals.size() == 1 ? awsPrincipals.get(0) : awsPrincipals);
        }
        if (!servicePrincipals.isEmpty()) {
            principal.put("Service", servicePrincipals.size() == 1 ? servicePrincipals.get(0) : servicePrincipals);
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
    protected void doAttachInlinePolicy(PolicyDocument policyDocument, String tenantId, String region, String resource) {
        throw new UnsupportedOperationException();
    }

    /**
     * Get inline policy document attached to an IAM role.
     *
     * @param identityName the IAM role name
     * @param policyName the name of the inline policy to retrieve.
     * @param roleName the IAM role name that has the inline policy attached.
     * @param tenantId the AWS Account ID.
     * @param region the AWS region for the IAM client.
     *
     * @return the inline policy document as a JSON string
     *
     * @throws software.amazon.awssdk.services.iam.model.NoSuchEntityException if the role or policy does not exist
     * @throws software.amazon.awssdk.services.iam.model.IamException for other IAM service errors
     */
    @Override
    protected String doGetInlinePolicyDetails(String identityName, String policyName, String roleName, String tenantId, String region) {
        if (StringUtils.isBlank(identityName)) {
            throw new InvalidArgumentException("identityName is required for AWS IAM");
        }

        if (StringUtils.isBlank(policyName)) {
            throw new InvalidArgumentException("policyName is required for AWS IAM");
        }

        IamClient client = this.iamClient;
        GetRolePolicyRequest request = GetRolePolicyRequest.builder()
                .roleName(identityName)
                .policyName(policyName)
                .build();
        GetRolePolicyResponse response = client.getRolePolicy(request);
        return URLDecoder.decode(response.policyDocument(), StandardCharsets.UTF_8);
    }

    @Override
    protected List<String> doGetAttachedPolicies(String identityName, String tenantId, String region) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doRemovePolicy(String identityName, String policyName, String tenantId, String region) {
        throw new UnsupportedOperationException();
    }


    /**
     * Delete IAM Role.
     *
     * @param identityName the IAM role name.
     * @param tenantId the AWS Account ID.
     * @param region the AWS region for the IAM client.
     *
     * @throws software.amazon.awssdk.services.iam.model.NoSuchEntityException if the role does not exist
     * @throws software.amazon.awssdk.services.iam.model.DeleteConflictException if the role has attached policies
     * @throws software.amazon.awssdk.services.iam.model.IamException for other IAM service errors
     */
    @Override
    protected void doDeleteIdentity(String identityName, String tenantId, String region) {
        DeleteRoleRequest request = DeleteRoleRequest.builder()
                .roleName(identityName)
                .build();
        this.iamClient.deleteRole(request);
    }

    /**
     * Get IAM Role.
     *
     * @param identityName the IAM role name.
     * @param tenantId the AWS Account ID.
     * @param region the AWS region for the IAM client.
     * @return the IAM role ARN.
     *
     * @throws software.amazon.awssdk.services.iam.model.NoSuchEntityException if the role does not exist
     * @throws software.amazon.awssdk.services.iam.model.IamException for other IAM service errors
     */
    @Override
    protected String doGetIdentity(String identityName, String tenantId, String region) {
        GetRoleRequest request = GetRoleRequest.builder()
                .roleName(identityName)
                .build();
        GetRoleResponse response = this.iamClient.getRole(request);
        Role role = response.role();
        return role != null ? role.arn() : null;
    }
}
