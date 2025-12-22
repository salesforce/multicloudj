package com.salesforce.multicloudj.iam.aws;

import com.google.auto.service.AutoService;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import software.amazon.awssdk.services.iam.model.Role;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoService(AbstractIam.class)
public class AwsIam extends AbstractIam {

    private IamClient iamClient;

    public AwsIam(Builder builder) {
        super(builder);
        this.iamClient = builder.iamClient;
    }

    public  AwsIam() {
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
            String regionStr = builder.getRegion() != null ? builder.getRegion() : "aws-global";
            Region regionObj = Region.of(regionStr);
            IamClientBuilder b = IamClient.builder().region(regionObj);

            AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), regionObj);
            if (credentialsProvider != null) {
                b.credentialsProvider(credentialsProvider);
            }
            if (builder.getEndpoint() != null) {
                b.endpointOverride(builder.getEndpoint());
            }
            return b.build();
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
        return null;
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

    }

    /**
     * Creates a new AWS IAM role with an assume role trust policy and optional creation options.
     *
     * <p>This method creates an IAM role in AWS with a trust policy (AssumeRolePolicyDocument) that
     * specifies which principals are allowed to assume the role. If no trust configuration is provided,
     * the role defaults to trusting the same AWS account's root principal. The method is idempotent:
     * if the role already exists, it returns the existing role's ARN.
     *
     * <p><strong>Trust Policy Behavior:</strong>
     * <ul>
     *   <li>If trustConfig is empty: defaults to "arn:aws:iam::{tenantId}:root"</li>
     *   <li>AWS principals (ARNs starting with "arn:" or 12-digit account IDs) → Principal.AWS</li>
     *   <li>Service principals (e.g., "ec2.amazonaws.com") → Principal.Service</li>
     *   <li>Conditions from trustConfig are added to the trust policy Statement</li>
     * </ul>
     *
     * @param identityName the IAM role name (e.g., "MyApplicationRole"). Must follow AWS IAM role
     *                     naming constraints: alphanumeric and +=,.@-_ characters, max 64 chars.
     * @param description optional description for the role (can be null, defaults to empty string)
     * @param tenantId the AWS Account ID (e.g., "123456789012"). Used to construct the role ARN
     *                 and default trust policy if no trustConfig is provided.
     * @param region the AWS region for the IAM client (e.g., "us-east-1", "aws-global").
     *               Note: IAM is a global service, but region may be used for client configuration.
     * @param trustConfig optional trust configuration containing principals that should be allowed
     *                    to assume this role. Principals can be specified as:
     *                    - AWS IAM ARN: "arn:aws:iam::999999999999:role/TrustedRole"
     *                    - AWS Account ID: "999999999999" (converted to arn:aws:iam::999999999999:root)
     *                    - Service principal: "ec2.amazonaws.com", "lambda.amazonaws.com"
     *                    Conditions can also be specified (e.g., StringEquals on sts:ExternalId)
     * @param options optional creation options for the role:
     *                - path: IAM path for the role (e.g., "/service-roles/")
     *                - maxSessionDuration: maximum session duration in seconds (900-43200)
     *                - permissionBoundary: ARN of the permissions boundary policy
     * @return the IAM role ARN in the format: arn:aws:iam::{tenantId}:role/{identityName}
     *         or arn:aws:iam::{tenantId}:role{path}{identityName} if path is specified
     */
    @Override
    protected String doCreateIdentity(String identityName, String description, String tenantId, String region, Optional<TrustConfiguration> trustConfig, Optional<CreateOptions> options) {
        IamClient client = this.iamClient;
        String assumeRolePolicyDocument = buildAssumeRolePolicyDocument(tenantId, trustConfig);

        CreateRoleRequest.Builder requestBuilder = CreateRoleRequest.builder()
                .roleName(identityName)
                .assumeRolePolicyDocument(assumeRolePolicyDocument)
                .description(description != null ? description : "");

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
            CreateRoleResponse response = client.createRole(requestBuilder.build());
            return response.role().arn();
        } catch (EntityAlreadyExistsException e) {
            GetRoleResponse getRoleResponse = client.getRole(GetRoleRequest.builder().roleName(identityName).build());
            Role role = getRoleResponse.role();
            return role != null ? role.arn() : null;
        }
    }

    private String buildAssumeRolePolicyDocument(String tenantId, Optional<TrustConfiguration> trustConfig) {
        List<String> awsPrincipals = new ArrayList<>();
        List<String> servicePrincipals = new ArrayList<>();

        if (trustConfig.isPresent() && trustConfig.get().getTrustedPrincipals() != null
                && !trustConfig.get().getTrustedPrincipals().isEmpty()) {
            for (String p : trustConfig.get().getTrustedPrincipals()) {
                if (p == null || p.isBlank()) {
                    continue;
                }

                if (p.startsWith("arn:")) {
                    awsPrincipals.add(p);
                } else if (p.matches("\\d{12}")) {
                    awsPrincipals.add("arn:aws:iam::" + p + ":root");
                } else if (p.endsWith(".amazonaws.com")) {
                    servicePrincipals.add(p);
                } else {
                    awsPrincipals.add(p);
                }
            }
        }

        if (awsPrincipals.isEmpty() && servicePrincipals.isEmpty()) {
            awsPrincipals.add("arn:aws:iam::" + tenantId + ":root");
        }

        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("Version", "2012-10-17");

        Map<String, Object> stmt = new LinkedHashMap<>();
        stmt.put("Effect", "Allow");
        stmt.put("Action", "sts:AssumeRole");

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
            return new ObjectMapper().writeValueAsString(doc);
        } catch (JsonProcessingException e) {
            throw new SubstrateSdkException("Failed to serialize assume role policy document", e);
        }
    }

    @Override
    protected void doAttachInlinePolicy(PolicyDocument policyDocument, String tenantId, String region, String resource) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected String doGetInlinePolicyDetails(String identityName, String policyName, String roleName, String tenantId, String region) {
        throw new UnsupportedOperationException();
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
     * Deletes an AWS IAM role.
     *
     * <p>This method permanently removes an IAM role from AWS. The role must not have any
     * attached managed policies or inline policies before deletion. If policies are attached,
     * they must be detached/deleted first, otherwise the deletion will fail.
     *
     * @param identityName the IAM role name to delete (e.g., "MyApplicationRole")
     * @param tenantId the AWS Account ID (e.g., "123456789012"). Note: This parameter is not used
     *                 in the current implementation as the role name alone is sufficient for deletion,
     *                 but is kept for interface compatibility.
     * @param region the AWS region for the IAM client (e.g., "us-east-1").
     *               Note: IAM is a global service, but region may be used for client configuration.
     * @throws software.amazon.awssdk.services.iam.model.NoSuchEntityException if the role does not exist
     * @throws software.amazon.awssdk.services.iam.model.DeleteConflictException if the role has attached policies
     * @throws software.amazon.awssdk.services.iam.model.IamException for other IAM service errors
     */
    @Override
    protected void doDeleteIdentity(String identityName, String tenantId, String region) {
        IamClient client = this.iamClient;
        DeleteRoleRequest request = DeleteRoleRequest.builder()
                .roleName(identityName)
                .build();
        client.deleteRole(request);
    }

    /**
     * Retrieves metadata for an existing AWS IAM role and returns its ARN as the unique identifier.
     *
     * <p>This method fetches the IAM role by name and returns the role's Amazon Resource Name (ARN)
     * as the unique identifier. The method will propagate any AWS SDK exceptions (e.g., NoSuchEntityException)
     * to the caller for centralized error handling.
     *
     * @param identityName the IAM role name to retrieve (e.g., "MyApplicationRole")
     * @param tenantId the AWS Account ID (e.g., "123456789012"). Note: This parameter is not used
     *                 in the current implementation as the role name alone is sufficient for retrieval,
     *                 but is kept for interface compatibility.
     * @param region the AWS region for the IAM client (e.g., "us-east-1", "aws-global").
     *               Note: IAM is a global service, but region may be used for client configuration.
     * @return the IAM role ARN in the format: arn:aws:iam::{account-id}:role/{role-name}
     *         or arn:aws:iam::{account-id}:role{path}{role-name} if the role has a path
     * @throws software.amazon.awssdk.services.iam.model.NoSuchEntityException if the role does not exist
     * @throws software.amazon.awssdk.services.iam.model.IamException for other IAM service errors
     */
    @Override
    protected String doGetIdentity(String identityName, String tenantId, String region) {
        IamClient client = this.iamClient;
        GetRoleRequest request = GetRoleRequest.builder()
                .roleName(identityName)
                .build();
        GetRoleResponse response = client.getRole(request);
        Role role = response.role();
        return role != null ? role.arn() : null;
    }
}
