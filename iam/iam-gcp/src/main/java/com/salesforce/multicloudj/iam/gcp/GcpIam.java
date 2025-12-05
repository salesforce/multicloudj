package com.salesforce.multicloudj.iam.gcp;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.auto.service.AutoService;
import com.google.cloud.resourcemanager.v3.ProjectsClient;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.iam.admin.v1.CreateServiceAccountRequest;
import com.google.iam.admin.v1.DeleteServiceAccountRequest;
import com.google.iam.admin.v1.GetServiceAccountRequest;
import com.google.iam.admin.v1.ServiceAccount;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.gcp.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.gcp.GcpConstants;
import com.salesforce.multicloudj.iam.driver.AbstractIam;
import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.PolicyDocument;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AutoService(AbstractIam.class)
public class GcpIam extends AbstractIam {

	private static final String EFFECT_ALLOW = "Allow";

	private ProjectsClient projectsClient;
    private IAMClient iamClient;

	public GcpIam() {
		this(new Builder());
	}

	public GcpIam(Builder builder) {
		super(builder);
		this.projectsClient = builder.projectsClient;
        this.iamClient = builder.iamClient;
	}

    /**
     * Creates a new GCP service account with optional trust configuration.
     *
     * <p>This method creates a service account in the specified GCP project. If trust configuration
     * is provided, it also grants the roles/iam.serviceAccountTokenCreator role to the specified
     * trusted principals, enabling them to impersonate this service account.
     *
     * @param identityName the service account ID (e.g., "my-service-account"). This will be used
     *                     to construct the full email: {identityName}@{project-id}.iam.gserviceaccount.com
     * @param description optional description for the service account (can be null, defaults to empty string)
     * @param tenantId the GCP project ID (e.g., "my-project-123") or full project resource name
     *                 (e.g., "projects/my-project-123"). The "projects/" prefix is optional.
     * @param region the region (not used in GCP IAM as service accounts are global resources)
     * @param trustConfig optional trust configuration containing principals that should be granted
     *                    the roles/iam.serviceAccountTokenCreator role. Principals can be specified as:
     *                    - Service account email: "sa@project.iam.gserviceaccount.com"
     *                    - Formatted member: "serviceAccount:sa@project.iam.gserviceaccount.com"
     *                    - User: "user:user@example.com"
     *                    - Group: "group:group@example.com"
     * @param options optional creation options (currently unused for GCP)
     * @return the service account email address (unique identifier) in the format:
     *         {identityName}@{project-id}.iam.gserviceaccount.com
     */
    @Override
    protected String doCreateIdentity(String identityName, String description, String tenantId,
                                      String region, Optional<TrustConfiguration> trustConfig,
                                      Optional<CreateOptions> options) {
        // Build the project resource name in the format "projects/{project-id}"
        String projectName = tenantId.startsWith("projects/") ? tenantId : "projects/" + tenantId;

        // Create the service account
        ServiceAccount serviceAccount = ServiceAccount.newBuilder()
                .setDisplayName(identityName)
                .setDescription(description != null ? description : "")
                .build();

        CreateServiceAccountRequest createRequest = CreateServiceAccountRequest.newBuilder()
                .setName(projectName)
                .setAccountId(identityName)
                .setServiceAccount(serviceAccount)
                .build();

        ServiceAccount createdServiceAccount;

        try {
            createdServiceAccount = iamClient.createServiceAccount(createRequest);
        } catch (AlreadyExistsException e) {
            // do not fail if service account already exists
            createdServiceAccount = this.getServiceAccount(identityName, tenantId);
        }

        String serviceAccountEmail = createdServiceAccount.getEmail();

        // If trust configuration is provided, add IAM bindings for roles/iam.serviceAccountTokenCreator
        if (trustConfig.isPresent() && !trustConfig.get().getTrustedPrincipals().isEmpty()) {
            String serviceAccountResourceName = createdServiceAccount.getName();

            // Get current IAM policy for the service account
            GetIamPolicyRequest getRequest = GetIamPolicyRequest.newBuilder()
                    .setResource(serviceAccountResourceName)
                    .build();

            Policy policy = iamClient.getIamPolicy(getRequest);
            if (policy == null) {
                policy = Policy.newBuilder().build();
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
	 * Attaches an inline policy to a resource.
	 * This implementation treats each action in the PolicyDocument statement as a GCP IAM role name
	 * and grants that role to the IAM member. The action values are used directly as role names
	 * (e.g., "roles/iam.serviceAccountUser", "roles/storage.objectViewer").
	 *
	 * <p>Note: GCP IAM is deny-by-default: access is denied unless explicitly allowed via bindings.
	 * <p>Note: This implementation only processes "Allow" statements. "Deny" statements are skipped because
	 * ProjectsClient only supports allow policies (bindings). Deny policies require the IAM v2 API
	 * (PoliciesClient) and are managed separately from allow policies.
	 *
	 * @param policyDocument the policy document where actions are treated as GCP IAM role names
	 * @param tenantId the resource name that owns the IAM policy. Examples include:
	 *		 "organizations/123456789012",
	 *		 "folders/987654321098",
	 *		 "projects/my-project",
	 *		 "projects/my-project/topics/my-topic",,
	 *		 Can be any GCP resource that supports IAM policies.
	 * @param region the region (optional for GCP)
	 * @param resource the IAM member (e.g., "serviceAccount:my-sa@project.iam.gserviceaccount.com",
	 *		 "user:user@example.com", "group:group@example.com")
	 */
	@Override
	protected void doAttachInlinePolicy(PolicyDocument policyDocument, String tenantId, String region, String resource) {
		// Get the current IAM policy for the resource
		GetIamPolicyRequest getRequest = GetIamPolicyRequest.newBuilder()
			.setResource(tenantId)
			.build();
		Policy policy = projectsClient.getIamPolicy(getRequest);

		if (policy == null) {
			policy = Policy.newBuilder().build();
		}

		// Store original policy to compare later
		Policy originalPolicy = policy;

		// Process each statement in the policy document
		for (Statement statement : policyDocument.getStatements()) {
			// Skip Deny statements: ProjectsClient only supports allow policies (bindings).
			// Deny policies require the IAM v2 API (PoliciesClient) and are managed separately.
			if (!EFFECT_ALLOW.equalsIgnoreCase(statement.getEffect())) {
				continue;
			}

			// Treat each action as a GCP IAM role name
			for (String action : statement.getActions()) {
				policy = addBinding(policy, action, resource);
			}
		}

		// Only make the remote call if the policy actually changed
		if (originalPolicy.getBindingsCount() == policy.getBindingsCount()
				&& originalPolicy.getBindingsList().equals(policy.getBindingsList())) {
			// Policy didn't change (all members already existed), skip the remote call
			return;
		}

		// Set the updated policy back to the resource
		SetIamPolicyRequest setRequest = SetIamPolicyRequest.newBuilder()
			.setResource(tenantId)
			.setPolicy(policy)
			.build();
		projectsClient.setIamPolicy(setRequest);
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

	/**
	 * Retrieves the details of a specific inline policy (role) attached to an IAM member.
	 * In GCP, this retrieves the role binding information and converts it back to a PolicyDocument format.
	 *
	 * @param identityName the IAM member (e.g., "serviceAccount:my-sa@project.iam.gserviceaccount.com",
	 *		 "user:user@example.com", "group:group@example.com")
	 * @param policyName the role name (e.g., "roles/iam.serviceAccountUser")
	 * @param tenantId the resource name that owns the IAM policy. Examples include:
	 *		 "organizations/123456789012",
	 *		 "folders/987654321098",
	 *		 "projects/my-project",
	 *		 "projects/my-project/topics/my-topic",,
	 *		 Can be any GCP resource that supports IAM policies.
	 * @param region the region (optional for GCP)
	 * @return the policy document as a JSON string, or null if the policy doesn't exist
	 */
	@Override
	protected String doGetInlinePolicyDetails(String identityName, String policyName, String tenantId, String region) {
		// Get the current IAM policy for the resource
		GetIamPolicyRequest getRequest = GetIamPolicyRequest.newBuilder()
			.setResource(tenantId)
			.build();
		Policy policy = projectsClient.getIamPolicy(getRequest);

		if (policy == null) {
			return null;
		}

		// Find the binding for the specified role
		Optional<Binding> binding = policy.getBindingsList().stream()
			.filter(b -> b.getRole().equals(policyName))
			.findFirst();

		// Check if the service account is a member of this binding
		if (!binding.isPresent() || !binding.get().getMembersList().contains(identityName)) {
			return null;
		}

		// Build a PolicyDocument to represent this role binding
		// The version field is immaterial for GCP IAM policy document.
		PolicyDocument policyDocument = PolicyDocument.builder()
			.version("")
			.statement(Statement.builder()
				.effect(EFFECT_ALLOW)
				.action(policyName)
				.build())
			.build();

		// Convert PolicyDocument to JSON string using Jackson
		return toJsonString(policyDocument);
	}

	/**
	 * Converts a PolicyDocument to its JSON string representation.
	 * Uses Jackson to serialize the document directly.
	 *
	 * @param policyDocument the policy document to serialize
	 * @return the JSON string representation
	 */
	private String toJsonString(PolicyDocument policyDocument) {
		try {
			return new ObjectMapper().writeValueAsString(policyDocument);
		} catch (Exception e) {
			throw new SubstrateSdkException("Failed to serialize policy document to JSON", e);
		}
	}

	/**
	 * Retrieves all policies (roles) attached to an IAM member.
	 * In GCP, "attached policies" are the IAM roles that have been granted to the IAM member
	 * through bindings in the resource's IAM policy.
	 *
	 * @param identityName the IAM member (e.g., "serviceAccount:my-sa@project.iam.gserviceaccount.com",
	 *		 "user:user@example.com", "group:group@example.com")
	 * @param tenantId the resource name that owns the IAM policy. Examples include:
	 *		 "organizations/123456789012",
	 *		 "folders/987654321098",
	 *		 "projects/my-project",
	 *		 "projects/my-project/topics/my-topic",,
	 *		 Can be any GCP resource that supports IAM policies.
	 * @param region the region (optional for GCP)
	 * @return a list of role names (e.g., "roles/iam.serviceAccountUser", "roles/storage.objectViewer")
	 */
	@Override
	protected List<String> doGetAttachedPolicies(String identityName, String tenantId, String region) {
		// Get the current IAM policy for the resource
		GetIamPolicyRequest getRequest = GetIamPolicyRequest.newBuilder()
			.setResource(tenantId)
			.build();
		Policy policy = projectsClient.getIamPolicy(getRequest);
		if (policy == null) {
			return List.of();
		}
		return policy.getBindingsList().stream()
			.filter(binding -> binding.getMembersList().contains(identityName))
			.map(Binding::getRole)
			.collect(Collectors.toList());
	}

	/**
	 * Removes an inline policy (role) from an IAM member.
	 * In GCP, this removes the IAM member from the specified role binding in the resource's IAM policy.
	 *
	 * @param identityName the IAM member (e.g., "serviceAccount:my-sa@project.iam.gserviceaccount.com",
	 *		 "user:user@example.com", "group:group@example.com")
	 * @param policyName the role name to remove (e.g., "roles/iam.serviceAccountUser")
	 * @param tenantId the resource name that owns the IAM policy. Examples include:
	 *		 "organizations/123456789012",
	 *		 "folders/987654321098",
	 *		 "projects/my-project",
	 *		 "projects/my-project/topics/my-topic",,
	 *		 Can be any GCP resource that supports IAM policies.
	 * @param region the region (optional for GCP)
	 */
	@Override
	protected void doRemovePolicy(String identityName, String policyName, String tenantId, String region) {
		// Get the current IAM policy for the resource
		GetIamPolicyRequest getRequest = GetIamPolicyRequest.newBuilder()
			.setResource(tenantId)
			.build();
		Policy policy = projectsClient.getIamPolicy(getRequest);

		if (policy == null) {
			// No policy exists, nothing to remove
			return;
		}

		// Check if the member is actually in the binding before attempting removal
		Optional<Binding> existingBinding = policy.getBindingsList().stream()
			.filter(binding -> binding.getRole().equals(policyName))
			.findFirst();

		if (!existingBinding.isPresent() || !existingBinding.get().getMembersList().contains(identityName)) {
			// Binding doesn't exist or member is not in the binding, nothing to remove
			return;
		}

		// Remove the binding - we know a change will occur because of the check above
		Policy updatedPolicy = removeBinding(policy, policyName, identityName);

		// Set the updated policy back to the resource
		SetIamPolicyRequest setRequest = SetIamPolicyRequest.newBuilder()
			.setResource(tenantId)
			.setPolicy(updatedPolicy)
			.build();
		projectsClient.setIamPolicy(setRequest);
	}

	/**
	 * Removes a member from a role binding in the IAM policy.
	 * If the binding becomes empty after removal, the entire binding is removed.
	 *
	 * @param policy the current IAM policy
	 * @param role the IAM role to remove the member from (e.g., "roles/iam.serviceAccountUser")
	 * @param member the member (service account) to remove from the role
	 * @return the updated policy with the member removed from the binding
	 */
	private Policy removeBinding(Policy policy, String role, String member) {
		// Find existing binding for this role
		Optional<Binding> existingBinding = policy.getBindingsList().stream()
			.filter(binding -> binding.getRole().equals(role))
			.findFirst();

		if (!existingBinding.isPresent()) {
			// Binding doesn't exist, nothing to remove
			return policy;
		}

		Binding binding = existingBinding.get();
		if (!binding.getMembersList().contains(member)) {
			// Member is not in this binding, nothing to remove
			return policy;
		}

		// Remove the member from the binding
		List<String> updatedMembers = binding.getMembersList().stream()
			.filter(m -> !m.equals(member))
			.collect(Collectors.toList());

		// If no members remain, remove the entire binding
		if (updatedMembers.isEmpty()) {
			List<Binding> updatedBindings = policy.getBindingsList().stream()
				.filter(b -> !b.getRole().equals(role))
				.collect(Collectors.toList());
			return policy.toBuilder()
				.clearBindings()
				.addAllBindings(updatedBindings)
				.build();
		}

		// Update the binding with remaining members
		Binding updatedBinding = binding.toBuilder()
			.clearMembers()
			.addAllMembers(updatedMembers)
			.build();

		List<Binding> updatedBindings = policy.getBindingsList().stream()
			.map(b -> b.getRole().equals(role) ? updatedBinding : b)
			.collect(Collectors.toList());

		return policy.toBuilder()
			.clearBindings()
			.addAllBindings(updatedBindings)
			.build();
	}

    /**
     * Deletes a service account from the specified GCP project.
     *
     * <p>This method permanently removes a service account and all its associated IAM bindings.
     * The operation cannot be undone. The method accepts either a service account ID or full
     * email address as input and constructs the appropriate resource name for the API call.
     *
     * @param identityName the service account identifier to delete, which can be:
     *                     - Service account ID: "my-service-account"
     *                     - Full email: "my-service-account@project-id.iam.gserviceaccount.com"
     *                     Both formats are accepted and will be normalized to the full resource name.
     * @param tenantId the GCP project ID (e.g., "my-project-123") or full project resource name
     *                 (e.g., "projects/my-project-123"). The "projects/" prefix is optional.
     * @param region the region (not used in GCP IAM as service accounts are global resources)
     * @throws ApiException if the service account is not found, access is denied, or deletion fails
     *                      (propagates to IamClient)
     */
    @Override
    protected void doDeleteIdentity(String identityName, String tenantId, String region) {
        // Build the project resource name in the format "projects/{project-id}"
        String serviceAccountResourceName = getServiceAccountResourceName(identityName, tenantId);

        // Delete the service account
        DeleteServiceAccountRequest deleteRequest = DeleteServiceAccountRequest.newBuilder()
                .setName(serviceAccountResourceName)
                .build();

        iamClient.deleteServiceAccount(deleteRequest);
    }

    /**
     * Retrieves service account metadata from the specified GCP project.
     *
     * <p>This method fetches details of an existing service account and returns its email address
     * as the unique identifier. The method accepts either a service account ID or full email address
     * as input and constructs the appropriate resource name for the API call.
     *
     * @param identityName the service account identifier, which can be:
     *                     - Service account ID: "my-service-account"
     *                     - Full email: "my-service-account@project-id.iam.gserviceaccount.com"
     *                     Both formats are accepted and will be normalized to the full resource name.
     * @param tenantId the GCP project ID (e.g., "my-project-123") or full project resource name
     *                 (e.g., "projects/my-project-123"). The "projects/" prefix is optional.
     * @param region the region (not used in GCP IAM as service accounts are global resources)
     * @return the service account email address (unique identifier) in the format:
     *         {account-id}@{project-id}.iam.gserviceaccount.com
     * @throws ApiException if the service account is not found or access is denied (propagates to IamClient)
     */
    @Override
    protected String doGetIdentity(String identityName, String tenantId, String region) {
        ServiceAccount serviceAccount = this.getServiceAccount(identityName, tenantId);
        // Return the service account email as the unique identifier
        return serviceAccount.getEmail();
    }

    private ServiceAccount getServiceAccount(String identityName, String tenantId) {
        // Build the project resource name in the format "projects/{project-id}"
        String serviceAccountResourceName = getServiceAccountResourceName(identityName, tenantId);

        // Get the service account
        GetServiceAccountRequest getRequest = GetServiceAccountRequest.newBuilder()
                .setName(serviceAccountResourceName)
                .build();

        return iamClient.getServiceAccount(getRequest);
    }

    private static String getServiceAccountResourceName(String identityName, String tenantId) {
        String projectName = tenantId.startsWith("projects/") ? tenantId : "projects/" + tenantId;

        // Build the service account resource name
        // Format: projects/{project-id}/serviceAccounts/{account-id}@{project-id}.iam.gserviceaccount.com
        String serviceAccountEmail = identityName.contains("@")
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

	@Override
	public void close() throws Exception {
		projectsClient.close();
        iamClient.close();
	}

	public static class Builder extends AbstractIam.Builder<GcpIam, Builder> {
		private ProjectsClient projectsClient;
        private IAMClient iamClient;

		public Builder() {
            providerId(GcpConstants.PROVIDER_ID);
        }

		public Builder withProjectsClient(ProjectsClient projectsClient) {
            this.projectsClient = projectsClient;
            return this;
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
                return IAMClient.create();
            } catch (Exception e) {
                throw new SubstrateSdkException("Could not create IAMClient", e);
            }
        }

		@Override
		public Builder self() {
			return this;
		}

		private static ProjectsClient buildProjectsClient(Builder builder) {
			try {
				return ProjectsClient.create();
			} catch (IOException e) {
				throw new SubstrateSdkException("Could not create ProjectsClient", e);
			}
		}

		@Override
		public GcpIam build() {
			if (projectsClient == null) {
				projectsClient = buildProjectsClient(this);
			}
                if (this.iamClient == null) {
                    this.iamClient = buildIamClient();
                }
			return new GcpIam(this);
		}
	}
}
