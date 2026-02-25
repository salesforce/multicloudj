package com.salesforce.multicloudj.iam.driver;

import com.salesforce.multicloudj.iam.model.CreateOptions;
import com.salesforce.multicloudj.iam.model.GetAttachedPoliciesRequest;
import com.salesforce.multicloudj.iam.model.GetInlinePolicyDetailsRequest;
import com.salesforce.multicloudj.iam.model.AttachInlinePolicyRequest;
import com.salesforce.multicloudj.iam.model.TrustConfiguration;

import java.util.List;
import java.util.Optional;

/**
 * Identity defines the contract for IAM operations across cloud providers.
 * This interface represents the provider-agnostic API for managing identities
 * (roles, service accounts, etc.) and their associated policies.
 */
public interface Identity {

    /**
     * Creates a new identity (role or service account) in the cloud provider.
     *
     * @param identityName the name of the identity to create
     * @param description a description of the identity's purpose
     * @param tenantId the tenant/project/account ID
     * @param region the region where the identity should be created
     * @param trustConfig optional trust configuration (trust policy/principals)
     * @param options optional creation options (path, max session duration, etc.)
     * @return the unique identifier (ARN, ID, etc.) of the created identity
     */
    String createIdentity(String identityName, String description, String tenantId, String region,
                         Optional<TrustConfiguration> trustConfig, Optional<CreateOptions> options);

    /**
     * Attaches an inline policy to an identity.
     *
     * @param request the request containing policy document, tenant ID, region, and identity/role names
     */
    void attachInlinePolicy(AttachInlinePolicyRequest request);

    /**
     * Retrieves the details of an inline policy attached to an identity.
     *
     * @param request the request containing identity name, policy name, role name, tenant ID, and region.
     *                Policy name and role name are optional and subject to cloud semantics.
     * @return the policy document as a JSON string
     */
    String getInlinePolicyDetails(GetInlinePolicyDetailsRequest request);

    /**
     * Lists all policies attached to an identity.
     *
     * @param request the request containing identity name, tenant ID, and region
     * @return a list of policy names attached to the identity
     */
    List<String> getAttachedPolicies(GetAttachedPoliciesRequest request);

    /**
     * Removes a policy from an identity.
     *
     * @param identityName the name of the identity
     * @param policyName the name of the policy to remove
     * @param tenantId the tenant/project/account ID
     * @param region the region where the identity exists
     */
    void removePolicy(String identityName, String policyName, String tenantId, String region);

    /**
     * Deletes an identity from the cloud provider.
     *
     * @param identityName the name of the identity to delete
     * @param tenantId the tenant/project/account ID
     * @param region the region where the identity exists
     */
    void deleteIdentity(String identityName, String tenantId, String region);

    /**
     * Retrieves information about an identity.
     *
     * @param identityName the name of the identity
     * @param tenantId the tenant/project/account ID
     * @param region the region where the identity exists
     * @return identity information as a JSON string
     */
    String getIdentity(String identityName, String tenantId, String region);
}

