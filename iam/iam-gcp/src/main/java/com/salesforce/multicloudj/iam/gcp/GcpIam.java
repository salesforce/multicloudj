package com.salesforce.multicloudj.iam.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.auto.service.AutoService;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.admin.v1.IAMSettings;
import com.google.iam.admin.v1.CreateServiceAccountRequest;
import com.google.iam.admin.v1.DeleteServiceAccountRequest;
import com.google.iam.admin.v1.GetServiceAccountRequest;
import com.google.iam.admin.v1.ServiceAccount;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.api.gax.rpc.StatusCode.Code.NOT_FOUND;

@AutoService(AbstractIam.class)
public class GcpIam extends AbstractIam {
    private IAMClient iamClient;

    public GcpIam(Builder builder) {
        super(builder);
        this.iamClient = builder.iamClient;
    }

    @Override
    protected String doCreateIdentity(String identityName, String description, String tenantId,
                                      String region, Optional<TrustConfiguration> trustConfig,
                                      Optional<CreateOptions> options) {
        try {
            // Build the project resource name in the format "projects/{project-id}"
            final String projectName = tenantId.startsWith("projects/") ? tenantId : "projects/" + tenantId;
            
            // Create the service account
            final ServiceAccount serviceAccount = ServiceAccount.newBuilder()
                    .setDisplayName(identityName)
                    .setDescription(description != null ? description : "")
                    .build();
            
            final CreateServiceAccountRequest createRequest = CreateServiceAccountRequest.newBuilder()
                    .setName(projectName)
                    .setAccountId(identityName)
                    .setServiceAccount(serviceAccount)
                    .build();
            
            final ServiceAccount createdServiceAccount = iamClient.createServiceAccount(createRequest);
            final String serviceAccountEmail = createdServiceAccount.getEmail();
            
            // If trust configuration is provided, add IAM bindings for roles/iam.serviceAccountTokenCreator
            if (trustConfig.isPresent() && !trustConfig.get().getTrustedPrincipals().isEmpty()) {
                final String serviceAccountResourceName = createdServiceAccount.getName();
                
                // Get current IAM policy for the service account
                final GetIamPolicyRequest getRequest = GetIamPolicyRequest.newBuilder()
                        .setResource(serviceAccountResourceName)
                        .build();
                
                Policy policy;
                try {
                    policy = iamClient.getIamPolicy(getRequest);
                } catch (ApiException e) {
                    // If policy doesn't exist, create a new one
                    if (e.getStatusCode().getCode() == NOT_FOUND) {
                        policy = Policy.newBuilder().build();
                    } else {
                        throw e;
                    }
                }
                
                // Add binding for each trusted principal
                for (String principal : trustConfig.get().getTrustedPrincipals()) {
                    // Format principal as a GCP member (e.g., "serviceAccount:email@project.iam.gserviceaccount.com")
                    String member = formatPrincipalAsMember(principal);
                    policy = addBinding(policy, "roles/iam.serviceAccountTokenCreator", member);
                }
                
                // Set the updated policy
                SetIamPolicyRequest setRequest = SetIamPolicyRequest.newBuilder()
                        .setResource(serviceAccountResourceName)
                        .setPolicy(policy)
                        .build();
                
                iamClient.setIamPolicy(setRequest);
            }
            
            return serviceAccountEmail;
        } catch (ApiException e) {
            throw new SubstrateSdkException("Failed to create service account: " + e.getMessage(), e);
        }
    }
    
    /**
     * Formats a principal identifier as a GCP IAM member string.
     * If the principal is already in the correct format (e.g., "serviceAccount:email@..."),
     * returns it as-is. Otherwise, assumes it's a service account email and formats it.
     *
     * @param principal the principal identifier
     * @return the formatted member string
     */
    private String formatPrincipalAsMember(String principal) {
        if (principal.contains(":")) {
            // Already formatted (e.g., "serviceAccount:email@...", "user:email@...", etc.)
            return principal;
        }
        // Assume it's a service account email
        return "serviceAccount:" + principal;
    }

    /**
     * Adds a binding to the IAM policy, merging with existing bindings for the same role.
     * This prevents policy bloat and avoids hitting GCP's IAM policy size limits.
     *
     * @param policy the current IAM policy
     * @param role the IAM role to grant (e.g., "roles/iam.serviceAccountUser")
     * @param member the member (service account) to grant the role to
     * @return the updated policy with the new binding
     */
    private Policy addBinding(Policy policy, String role, String member) {
        // Find existing binding for this role
        Optional<Binding> existingBinding = policy.getBindingsList().stream()
                .filter(binding -> binding.getRole().equals(role))
                .findFirst();

        if (existingBinding.isPresent()) {
            // Update existing binding if member is not already present
            Binding binding = existingBinding.get();
            if (!binding.getMembersList().contains(member)) {
                Binding updatedBinding = binding.toBuilder().addMembers(member).build();
                List<Binding> updatedBindings = policy.getBindingsList().stream()
                        .map(b -> b.getRole().equals(role) ? updatedBinding : b)
                        .collect(Collectors.toList());

                return policy.toBuilder()
                        .clearBindings()
                        .addAllBindings(updatedBindings)
                        .build();
            }
            // Member already exists, return policy unchanged
            return policy;
        }

        // No existing binding for this role, create a new one
        return policy.toBuilder()
                .addBindings(Binding.newBuilder()
                        .setRole(role)
                        .addMembers(member)
                        .build())
                .build();
    }

    @Override
    protected void doAttachInlinePolicy(PolicyDocument policyDocument, String tenantId,
                                        String region, String resource) {
        // TODO: Implement GCP inline policy attachment
        throw new UnSupportedOperationException("doAttachInlinePolicy not yet implemented for GCP");
    }


    @Override
    protected String doGetInlinePolicyDetails(String identityName, String policyName,
                                              String tenantId, String region) {
        // TODO: Implement GCP inline policy details retrieval
        throw new UnSupportedOperationException("doGetInlinePolicyDetails not yet implemented for GCP");
    }

    @Override
    protected List<String> doGetAttachedPolicies(String identityName, String tenantId,
                                                 String region) {
        // TODO: Implement GCP attached policies retrieval
        throw new UnSupportedOperationException("doGetAttachedPolicies not yet implemented for GCP");
    }

    @Override
    protected void doRemovePolicy(String identityName, String policyName, String tenantId,
                                  String region) {
        // TODO: Implement GCP policy removal
        throw new UnSupportedOperationException("doRemovePolicy not yet implemented for GCP");
    }

    /**
     * Deletes a service account from the specified GCP project.
     * 
     * @param identityName the name of the identity (service account ID or email)
     * @param tenantId the tenant ID (GCP project ID)
     * @param region the region (not used in GCP IAM as service accounts are global)
     */
    @Override
    protected void doDeleteIdentity(String identityName, String tenantId, String region) {
        try {
            // Build the project resource name in the format "projects/{project-id}"
            final String serviceAccountResourceName = getServiceAccountResourceName(identityName, tenantId);

            // Delete the service account
            final DeleteServiceAccountRequest deleteRequest = DeleteServiceAccountRequest.newBuilder()
                    .setName(serviceAccountResourceName)
                    .build();
            
            iamClient.deleteServiceAccount(deleteRequest);
            
        } catch (ApiException e) {
            throw new SubstrateSdkException("Failed to delete service account: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new UnknownException("Failed to delete service account: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves service account metadata from project specified by tenantId. Returns a string with a unique identityId 
     * as service account email.
     * 
     * @param identityName the name of the identity (service account ID)
     * @param tenantId the tenant ID (GCP project ID)
     * @param region the region (not used in GCP IAM as service accounts are global)
     * @return the service account email (unique identifier)
     */
    @Override
    protected String doGetIdentity(String identityName, String tenantId, String region) {
        try {
            // Build the project resource name in the format "projects/{project-id}"
            final String serviceAccountResourceName = getServiceAccountResourceName(identityName, tenantId);

            // Get the service account
            final GetServiceAccountRequest getRequest = GetServiceAccountRequest.newBuilder()
                    .setName(serviceAccountResourceName)
                    .build();
            
            final ServiceAccount serviceAccount = iamClient.getServiceAccount(getRequest);
            
            // Return the service account email as the unique identifier
            return serviceAccount.getEmail();
            
        } catch (ApiException e) {
            throw new SubstrateSdkException("Failed to get service account: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new UnknownException("Failed to get service account: " + e.getMessage(), e);
        }
    }

    private static String getServiceAccountResourceName(String identityName, String tenantId) {
        final String projectName = tenantId.startsWith("projects/") ? tenantId : "projects/" + tenantId;

        // Build the service account resource name
        // Format: projects/{project-id}/serviceAccounts/{account-id}@{project-id}.iam.gserviceaccount.com
        final String serviceAccountEmail = identityName.contains("@")
            ? identityName
            : identityName + "@" + projectName.substring(9) + ".iam.gserviceaccount.com";

        return projectName + "/serviceAccounts/" + serviceAccountEmail;
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        if (t instanceof ApiException) {
            ApiException exception = (ApiException) t;
            return CommonErrorCodeMapping.getException(exception.getStatusCode().getCode());
        }
        return UnknownException.class;
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractIam.Builder<GcpIam, Builder> {
        /** The IAM Client to be used for operations */
        private IAMClient iamClient;

        /** Default constructor that sets the provider id */
        protected Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

        /**
         * Sets the IAM Client to be used for operations.
         *
         * @param iamClient
         * @return this builder for chaining
         */
        public Builder withIamClient(IAMClient iamClient) {
            this.iamClient = iamClient;
            return self();
        }

        /**
         * Builds a IAMClient from the current configuration.
         *
         * @return A configured IAMClient
         * @throws SubstrateSdkException If client creation fails
         */
        private IAMClient buildIamClient() {
            try {
                final IAMSettings.Builder settingsBuilder = IAMSettings.newBuilder();
                return IAMClient.create(settingsBuilder.build());
            } catch (Exception e) {
                throw new SubstrateSdkException("Failed to build IAMClient", e);
            }
        }

        @Override
        public Builder self() {
            return this;
        }

        /**
         * Builds an GcpIam instance with the current configuration.
         *
         * @return A new GcpIam instance
         * @throws SubstrateSdkException If store creation fails
         */
        @Override
        public GcpIam build() {
            if (this.iamClient == null) {
                this.iamClient = buildIamClient();
            }
            return new GcpIam(this);
        }
    }

    /**
     * Closes resources and connections associated with this GCP IAM client.
     * This method closes the IAM client if it was created.
     */
    @Override
    public void close() {
        this.iamClient.close();
    }
}