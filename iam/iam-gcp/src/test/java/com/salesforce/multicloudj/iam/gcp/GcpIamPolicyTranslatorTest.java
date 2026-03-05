package com.salesforce.multicloudj.iam.gcp;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.iam.model.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class GcpIamPolicyTranslatorTest {

    @Test
    void testTranslateStorageGetObjectAction() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(1, roles.size());
        assertEquals("roles/storage.objectViewer", roles.get(0));
    }

    @Test
    void testTranslateStoragePutObjectAction() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:PutObject")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(1, roles.size());
        assertEquals("roles/storage.objectCreator", roles.get(0));
    }

    @Test
    void testTranslateMultipleStorageActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .action("storage:PutObject")
            .action("storage:DeleteObject")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(3, roles.size());
        assertTrue(roles.contains("roles/storage.objectViewer"));
        assertTrue(roles.contains("roles/storage.objectCreator"));
        assertTrue(roles.contains("roles/storage.objectAdmin"));
    }

    @Test
    void testTranslateComputeActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("compute:CreateInstance")
            .action("compute:DeleteInstance")
            .action("compute:GetInstance")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(2, roles.size()); // CreateInstance and DeleteInstance map to same role
        assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
        assertTrue(roles.contains("roles/compute.viewer"));
    }

    @Test
    void testTranslateIamActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("iam:AssumeRole")
            .action("iam:CreateRole")
            .action("iam:GetRole")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(3, roles.size());
        assertTrue(roles.contains("roles/iam.serviceAccountUser"));
        assertTrue(roles.contains("roles/iam.serviceAccountAdmin"));
        assertTrue(roles.contains("roles/iam.serviceAccountViewer"));
    }

    @Test
    void testTranslateDuplicateActionsReturnUniqueRoles() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .action("storage:ListBucket") // Also maps to objectViewer
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(1, roles.size());
        assertEquals("roles/storage.objectViewer", roles.get(0));
    }

    @Test
    void testTranslateActionToRoleStorageActions() {
        assertEquals("roles/storage.objectViewer", 
            GcpIamPolicyTranslator.translateActionToRole("storage:GetObject"));
        assertEquals("roles/storage.objectCreator", 
            GcpIamPolicyTranslator.translateActionToRole("storage:PutObject"));
        assertEquals("roles/storage.objectAdmin", 
            GcpIamPolicyTranslator.translateActionToRole("storage:DeleteObject"));
        assertEquals("roles/storage.admin", 
            GcpIamPolicyTranslator.translateActionToRole("storage:CreateBucket"));
    }

    @Test
    void testTranslateActionToRoleComputeActions() {
        assertEquals("roles/compute.instanceAdmin.v1", 
            GcpIamPolicyTranslator.translateActionToRole("compute:CreateInstance"));
        assertEquals("roles/compute.instanceAdmin.v1", 
            GcpIamPolicyTranslator.translateActionToRole("compute:DeleteInstance"));
        assertEquals("roles/compute.viewer", 
            GcpIamPolicyTranslator.translateActionToRole("compute:GetInstance"));
    }

    @Test
    void testTranslateActionToRoleIamActions() {
        assertEquals("roles/iam.serviceAccountUser", 
            GcpIamPolicyTranslator.translateActionToRole("iam:AssumeRole"));
        assertEquals("roles/iam.serviceAccountAdmin", 
            GcpIamPolicyTranslator.translateActionToRole("iam:CreateRole"));
        assertEquals("roles/iam.serviceAccountViewer", 
            GcpIamPolicyTranslator.translateActionToRole("iam:GetRole"));
    }

    @Test
    void testTranslateUnknownActionThrowsException() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("unknown:Action")
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            GcpIamPolicyTranslator.translateActionsToRoles(statement);
        });

        assertTrue(exception.getMessage().contains("Unknown substrate-neutral action: unknown:Action"));
    }

    @Test
    void testTranslateActionToRoleUnknownActionThrowsException() {
        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            GcpIamPolicyTranslator.translateActionToRole("invalid:Action");
        });

        assertTrue(exception.getMessage().contains("Unknown substrate-neutral action: invalid:Action"));
    }

    @Test
    void testTranslateActionsToRolesWithConditionsThrowsException() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .condition("stringEquals", "key", "value")
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            GcpIamPolicyTranslator.translateActionsToRoles(statement);
        });

        assertTrue(exception.getMessage().contains("GCP IAM policy conditions are not yet supported"));
    }

    @Test
    void testTranslateAllStorageActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .action("storage:PutObject")
            .action("storage:DeleteObject")
            .action("storage:ListBucket")
            .action("storage:GetBucketLocation")
            .action("storage:CreateBucket")
            .action("storage:DeleteBucket")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        // Verify all unique roles are present
        assertTrue(roles.contains("roles/storage.objectViewer"));
        assertTrue(roles.contains("roles/storage.objectCreator"));
        assertTrue(roles.contains("roles/storage.objectAdmin"));
        assertTrue(roles.contains("roles/storage.admin"));
    }

    @Test
    void testTranslateAllComputeActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("compute:CreateInstance")
            .action("compute:DeleteInstance")
            .action("compute:StartInstance")
            .action("compute:StopInstance")
            .action("compute:DescribeInstances")
            .action("compute:GetInstance")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        // Verify all unique roles are present
        assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
        assertTrue(roles.contains("roles/compute.viewer"));
    }

    @Test
    void testTranslateAllIamActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("iam:AssumeRole")
            .action("iam:CreateRole")
            .action("iam:DeleteRole")
            .action("iam:GetRole")
            .action("iam:AttachRolePolicy")
            .action("iam:DetachRolePolicy")
            .action("iam:PutRolePolicy")
            .action("iam:GetRolePolicy")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        // Verify all unique roles are present
        assertTrue(roles.contains("roles/iam.serviceAccountUser"));
        assertTrue(roles.contains("roles/iam.serviceAccountAdmin"));
        assertTrue(roles.contains("roles/iam.serviceAccountViewer"));
    }

    @Test
    void testTranslateMixedServiceActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:GetObject")
            .action("compute:CreateInstance")
            .action("iam:AssumeRole")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(3, roles.size());
        assertTrue(roles.contains("roles/storage.objectViewer"));
        assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
        assertTrue(roles.contains("roles/iam.serviceAccountUser"));
    }

    @Test
    void testTranslateWildcardStorageAction() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:*")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(1, roles.size());
        assertEquals("roles/storage.admin", roles.get(0));
    }

    @Test
    void testTranslateWildcardComputeAction() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("compute:*")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(1, roles.size());
        assertEquals("roles/compute.admin", roles.get(0));
    }

    @Test
    void testTranslateWildcardIamAction() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("iam:*")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(1, roles.size());
        assertEquals("roles/iam.serviceAccountAdmin", roles.get(0));
    }

    @Test
    void testTranslateMixedWildcardAndSpecificActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:*")
            .action("compute:CreateInstance")
            .action("iam:GetRole")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(3, roles.size());
        assertTrue(roles.contains("roles/storage.admin"));
        assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
        assertTrue(roles.contains("roles/iam.serviceAccountViewer"));
    }

    @Test
    void testTranslateMultipleWildcardActions() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("storage:*")
            .action("compute:*")
            .action("iam:*")
            .build();

        List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

        assertEquals(3, roles.size());
        assertTrue(roles.contains("roles/storage.admin"));
        assertTrue(roles.contains("roles/compute.admin"));
        assertTrue(roles.contains("roles/iam.serviceAccountAdmin"));
    }

    @Test
    void testTranslateUnknownWildcardServiceThrowsException() {
        Statement statement = Statement.builder()
            .effect("Allow")
            .action("unknown:*")
            .build();

        SubstrateSdkException exception = assertThrows(SubstrateSdkException.class, () -> {
            GcpIamPolicyTranslator.translateActionsToRoles(statement);
        });

        assertTrue(exception.getMessage().contains("Unknown substrate-neutral service for wildcard action: unknown:*"));
        assertTrue(exception.getMessage().contains("Supported wildcard services: storage:*, compute:*, iam:*"));
    }

    @Test
    void testTranslateActionToRoleWildcardStorage() {
        assertEquals("roles/storage.admin", 
            GcpIamPolicyTranslator.translateActionToRole("storage:*"));
    }

    @Test
    void testTranslateActionToRoleWildcardCompute() {
        assertEquals("roles/compute.admin", 
            GcpIamPolicyTranslator.translateActionToRole("compute:*"));
    }

    @Test
    void testTranslateActionToRoleWildcardIam() {
        assertEquals("roles/iam.serviceAccountAdmin", 
            GcpIamPolicyTranslator.translateActionToRole("iam:*"));
    }
}
