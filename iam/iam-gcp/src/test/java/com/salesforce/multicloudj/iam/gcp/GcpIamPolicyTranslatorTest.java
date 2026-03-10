package com.salesforce.multicloudj.iam.gcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.iam.model.Action;
import com.salesforce.multicloudj.iam.model.ComputeActions;
import com.salesforce.multicloudj.iam.model.ConditionOperator;
import com.salesforce.multicloudj.iam.model.Effect;
import com.salesforce.multicloudj.iam.model.IamActions;
import com.salesforce.multicloudj.iam.model.Statement;
import com.salesforce.multicloudj.iam.model.StorageActions;
import java.util.List;
import org.junit.jupiter.api.Test;

public class GcpIamPolicyTranslatorTest {

  @Test
  void testTranslateStorageGetObjectAction() {
    Statement statement =
        Statement.builder().effect(Effect.ALLOW).action(StorageActions.GET_OBJECT).build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(1, roles.size());
    assertEquals("roles/storage.objectViewer", roles.get(0));
  }

  @Test
  void testTranslateStoragePutObjectAction() {
    Statement statement =
        Statement.builder().effect(Effect.ALLOW).action(StorageActions.PUT_OBJECT).build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(1, roles.size());
    assertEquals("roles/storage.objectCreator", roles.get(0));
  }

  @Test
  void testTranslateMultipleStorageActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(StorageActions.PUT_OBJECT)
            .action(StorageActions.DELETE_OBJECT)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(3, roles.size());
    assertTrue(roles.contains("roles/storage.objectViewer"));
    assertTrue(roles.contains("roles/storage.objectCreator"));
    assertTrue(roles.contains("roles/storage.objectAdmin"));
  }

  @Test
  void testTranslateComputeActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(ComputeActions.CREATE_INSTANCE)
            .action(ComputeActions.DELETE_INSTANCE)
            .action(ComputeActions.GET_INSTANCE)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(2, roles.size()); // CreateInstance and DeleteInstance map to same role
    assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
    assertTrue(roles.contains("roles/compute.viewer"));
  }

  @Test
  void testTranslateIamActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(IamActions.ASSUME_ROLE)
            .action(IamActions.CREATE_ROLE)
            .action(IamActions.GET_ROLE)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(3, roles.size());
    assertTrue(roles.contains("roles/iam.serviceAccountUser"));
    assertTrue(roles.contains("roles/iam.serviceAccountAdmin"));
    assertTrue(roles.contains("roles/iam.serviceAccountViewer"));
  }

  @Test
  void testTranslateDuplicateActionsReturnUniqueRoles() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(StorageActions.LIST_BUCKET) // Also maps to objectViewer
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(1, roles.size());
    assertEquals("roles/storage.objectViewer", roles.get(0));
  }

  @Test
  void testTranslateActionToRoleStorageActions() {
    assertEquals(
        "roles/storage.objectViewer",
        GcpIamPolicyTranslator.translateActionToRole(StorageActions.GET_OBJECT));
    assertEquals(
        "roles/storage.objectCreator",
        GcpIamPolicyTranslator.translateActionToRole(StorageActions.PUT_OBJECT));
    assertEquals(
        "roles/storage.objectAdmin",
        GcpIamPolicyTranslator.translateActionToRole(StorageActions.DELETE_OBJECT));
    assertEquals(
        "roles/storage.admin",
        GcpIamPolicyTranslator.translateActionToRole(StorageActions.CREATE_BUCKET));
  }

  @Test
  void testTranslateActionToRoleComputeActions() {
    assertEquals(
        "roles/compute.instanceAdmin.v1",
        GcpIamPolicyTranslator.translateActionToRole(ComputeActions.CREATE_INSTANCE));
    assertEquals(
        "roles/compute.instanceAdmin.v1",
        GcpIamPolicyTranslator.translateActionToRole(ComputeActions.DELETE_INSTANCE));
    assertEquals(
        "roles/compute.viewer",
        GcpIamPolicyTranslator.translateActionToRole(ComputeActions.GET_INSTANCE));
  }

  @Test
  void testTranslateActionToRoleIamActions() {
    assertEquals(
        "roles/iam.serviceAccountUser",
        GcpIamPolicyTranslator.translateActionToRole(IamActions.ASSUME_ROLE));
    assertEquals(
        "roles/iam.serviceAccountAdmin",
        GcpIamPolicyTranslator.translateActionToRole(IamActions.CREATE_ROLE));
    assertEquals(
        "roles/iam.serviceAccountViewer",
        GcpIamPolicyTranslator.translateActionToRole(IamActions.GET_ROLE));
  }

  @Test
  void testTranslateUnknownActionThrowsException() {
    Statement statement =
        Statement.builder().effect(Effect.ALLOW).action(Action.of("unknown:Action")).build();

    InvalidArgumentException exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> {
              GcpIamPolicyTranslator.translateActionsToRoles(statement);
            });

    assertTrue(exception.getMessage().contains("Unknown substrate-neutral action: unknown:Action"));
  }

  @Test
  void testTranslateActionToRoleUnknownActionThrowsException() {
    InvalidArgumentException exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> {
              GcpIamPolicyTranslator.translateActionToRole(Action.of("invalid:Action"));
            });

    assertTrue(exception.getMessage().contains("Unknown substrate-neutral action: invalid:Action"));
  }

  @Test
  void testTranslateActionsToRolesWithConditionsThrowsException() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .condition(ConditionOperator.STRING_EQUALS, "key", "value")
            .build();

    InvalidArgumentException exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> {
              GcpIamPolicyTranslator.translateActionsToRoles(statement);
            });

    assertTrue(exception.getMessage().contains("GCP IAM policy conditions are not yet supported"));
  }

  @Test
  void testTranslateAllStorageActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(StorageActions.PUT_OBJECT)
            .action(StorageActions.DELETE_OBJECT)
            .action(StorageActions.LIST_BUCKET)
            .action(StorageActions.GET_BUCKET_LOCATION)
            .action(StorageActions.CREATE_BUCKET)
            .action(StorageActions.DELETE_BUCKET)
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
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(ComputeActions.CREATE_INSTANCE)
            .action(ComputeActions.DELETE_INSTANCE)
            .action(ComputeActions.START_INSTANCE)
            .action(ComputeActions.STOP_INSTANCE)
            .action(ComputeActions.DESCRIBE_INSTANCES)
            .action(ComputeActions.GET_INSTANCE)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    // Verify all unique roles are present
    assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
    assertTrue(roles.contains("roles/compute.viewer"));
  }

  @Test
  void testTranslateAllIamActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(IamActions.ASSUME_ROLE)
            .action(IamActions.CREATE_ROLE)
            .action(IamActions.DELETE_ROLE)
            .action(IamActions.GET_ROLE)
            .action(IamActions.ATTACH_ROLE_POLICY)
            .action(IamActions.DETACH_ROLE_POLICY)
            .action(IamActions.PUT_ROLE_POLICY)
            .action(IamActions.GET_ROLE_POLICY)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    // Verify all unique roles are present
    assertTrue(roles.contains("roles/iam.serviceAccountUser"));
    assertTrue(roles.contains("roles/iam.serviceAccountAdmin"));
    assertTrue(roles.contains("roles/iam.serviceAccountViewer"));
  }

  @Test
  void testTranslateMixedServiceActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.GET_OBJECT)
            .action(ComputeActions.CREATE_INSTANCE)
            .action(IamActions.ASSUME_ROLE)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(3, roles.size());
    assertTrue(roles.contains("roles/storage.objectViewer"));
    assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
    assertTrue(roles.contains("roles/iam.serviceAccountUser"));
  }

  @Test
  void testTranslateWildcardStorageAction() {
    Statement statement =
        Statement.builder().effect(Effect.ALLOW).action(StorageActions.ALL).build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(1, roles.size());
    assertEquals("roles/storage.admin", roles.get(0));
  }

  @Test
  void testTranslateWildcardComputeAction() {
    Statement statement =
        Statement.builder().effect(Effect.ALLOW).action(ComputeActions.ALL).build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(1, roles.size());
    assertEquals("roles/compute.admin", roles.get(0));
  }

  @Test
  void testTranslateWildcardIamAction() {
    Statement statement = Statement.builder().effect(Effect.ALLOW).action(IamActions.ALL).build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(1, roles.size());
    assertEquals("roles/iam.serviceAccountAdmin", roles.get(0));
  }

  @Test
  void testTranslateMixedWildcardAndSpecificActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.ALL)
            .action(ComputeActions.CREATE_INSTANCE)
            .action(IamActions.GET_ROLE)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(3, roles.size());
    assertTrue(roles.contains("roles/storage.admin"));
    assertTrue(roles.contains("roles/compute.instanceAdmin.v1"));
    assertTrue(roles.contains("roles/iam.serviceAccountViewer"));
  }

  @Test
  void testTranslateMultipleWildcardActions() {
    Statement statement =
        Statement.builder()
            .effect(Effect.ALLOW)
            .action(StorageActions.ALL)
            .action(ComputeActions.ALL)
            .action(IamActions.ALL)
            .build();

    List<String> roles = GcpIamPolicyTranslator.translateActionsToRoles(statement);

    assertEquals(3, roles.size());
    assertTrue(roles.contains("roles/storage.admin"));
    assertTrue(roles.contains("roles/compute.admin"));
    assertTrue(roles.contains("roles/iam.serviceAccountAdmin"));
  }

  @Test
  void testTranslateUnknownWildcardServiceThrowsException() {
    Statement statement =
        Statement.builder().effect(Effect.ALLOW).action(Action.of("unknown:*")).build();

    InvalidArgumentException exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> {
              GcpIamPolicyTranslator.translateActionsToRoles(statement);
            });

    assertTrue(
        exception
            .getMessage()
            .contains("Unknown substrate-neutral service for wildcard action: unknown:*"));
    assertTrue(
        exception
            .getMessage()
            .contains("Supported wildcard services: storage:*, compute:*, iam:*"));
  }

  @Test
  void testTranslateActionToRoleWildcardStorage() {
    assertEquals(
        "roles/storage.admin", GcpIamPolicyTranslator.translateActionToRole(StorageActions.ALL));
  }

  @Test
  void testTranslateActionToRoleWildcardCompute() {
    assertEquals(
        "roles/compute.admin", GcpIamPolicyTranslator.translateActionToRole(ComputeActions.ALL));
  }

  @Test
  void testTranslateActionToRoleWildcardIam() {
    assertEquals(
        "roles/iam.serviceAccountAdmin",
        GcpIamPolicyTranslator.translateActionToRole(IamActions.ALL));
  }

  @Test
  void testTranslateNullEffectThrowsException() {
    Statement mockStatement = mock(Statement.class);
    when(mockStatement.getEffect()).thenReturn(null);
    when(mockStatement.getActions())
        .thenReturn(java.util.Collections.singletonList(StorageActions.GET_OBJECT));

    InvalidArgumentException exception =
        assertThrows(
            InvalidArgumentException.class,
            () -> {
              GcpIamPolicyTranslator.translateActionsToRoles(mockStatement);
            });

    assertEquals("Effect is required for GCP IAM policy statement", exception.getMessage());
  }
}
