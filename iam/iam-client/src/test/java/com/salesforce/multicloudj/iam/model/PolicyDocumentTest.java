package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for PolicyDocument builder pattern.
 */
public class PolicyDocumentTest {

    private static final String TEST_VERSION = "TEST_VERSION";

    @Test
    public void testPolicyDocumentBuilder() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2024-01-01")
            .statement("StorageAccess")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addAction("storage:PutObject")
                .addPrincipal("arn:aws:iam::123456789012:user/ExampleUser")
                .addResource("storage://my-bucket/*")
                .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
            .endStatement()
            .build();

        assertEquals("2024-01-01", policy.getVersion());
        assertEquals(1, policy.getStatements().size());

        Statement statement = policy.getStatements().get(0);
        assertEquals("StorageAccess", statement.getSid());
        assertEquals("Allow", statement.getEffect());
        assertEquals(Arrays.asList("storage:GetObject", "storage:PutObject"), statement.getActions());
        assertEquals(Arrays.asList("arn:aws:iam::123456789012:user/ExampleUser"), statement.getPrincipals());
        assertEquals(Arrays.asList("storage://my-bucket/*"), statement.getResources());

        assertTrue(statement.getConditions().containsKey("StringEquals"));
        assertEquals("us-west-2", statement.getConditions().get("StringEquals").get("aws:RequestedRegion"));
    }

    @Test
    public void testMultipleStatements() {
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement("ReadAccess")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://my-bucket/*")
            .endStatement()
            .statement("WriteAccess")
                .effect("Allow")
                .addAction("storage:PutObject")
                .addResource("storage://my-bucket/*")
            .endStatement()
            .build();

        assertEquals(2, policy.getStatements().size());
        assertEquals("ReadAccess", policy.getStatements().get(0).getSid());
        assertEquals("WriteAccess", policy.getStatements().get(1).getSid());
    }

    @Test
    public void testAddCompleteStatement() {
        Statement statement = Statement.builder()
            .sid("TestStatement")
            .effect("Allow")
            .addAction("storage:GetObject")
            .addResource("storage://my-bucket/*")
            .build();

        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .addStatement(statement)
            .build();

        assertEquals(1, policy.getStatements().size());
        assertEquals("TestStatement", policy.getStatements().get(0).getSid());
    }

    @Test
    public void testEmptyPolicyThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            PolicyDocument.builder().build();
        });
    }

    @Test
    public void testMissingVersionThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            PolicyDocument.builder()
                .statement("TestStatement")
                    .effect("Allow")
                    .addAction("storage:GetObject")
                    .addResource("storage://test-bucket/*")
                .endStatement()
                .build();
        });
    }

    @Test
    public void testStatementWithoutEffectThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            PolicyDocument.builder()
                .version(TEST_VERSION)
                .statement("TestStatement")
                    .addAction("storage:GetObject")
                .endStatement()
                .build();
        });
    }

    @Test
    public void testStatementWithoutActionsThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            PolicyDocument.builder()
                .version(TEST_VERSION)
                .statement("TestStatement")
                    .effect("Allow")
                .endStatement()
                .build();
        });
    }

    @Test
    public void testVersionHandling() {
        // Test custom version
        PolicyDocument customVersionPolicy = PolicyDocument.builder()
            .version("2023-06-01")
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        assertEquals("2023-06-01", customVersionPolicy.getVersion());

        // Test default version
        PolicyDocument defaultVersionPolicy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        assertEquals(TEST_VERSION, defaultVersionPolicy.getVersion());
    }

    @Test
    public void testBuilderMethodsWithMultipleValues() {
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addAction("storage:PutObject")
                .addActions(Arrays.asList("storage:DeleteObject", "storage:ListObjects"))
                .addResource("storage://bucket1/*")
                .addResource("storage://bucket2/*")
                .addResources(Arrays.asList("storage://bucket3/*", "storage://bucket4/*"))
                .addPrincipal("principal1")
                .addPrincipal("principal2")
                .addPrincipals(Arrays.asList("principal3", "principal4"))
                .addCondition("StringEquals", "aws:RequestedRegion", "us-west-2")
                .addCondition("DateGreaterThan", "aws:CurrentTime", "2024-01-01T00:00:00Z")
            .endStatement()
            .build();

        Statement statement = policy.getStatements().get(0);

        // Test actions
        assertEquals(4, statement.getActions().size());
        assertTrue(statement.getActions().contains("storage:GetObject"));
        assertTrue(statement.getActions().contains("storage:PutObject"));
        assertTrue(statement.getActions().contains("storage:DeleteObject"));
        assertTrue(statement.getActions().contains("storage:ListObjects"));

        // Test resources
        assertEquals(4, statement.getResources().size());
        assertTrue(statement.getResources().contains("storage://bucket1/*"));
        assertTrue(statement.getResources().contains("storage://bucket2/*"));
        assertTrue(statement.getResources().contains("storage://bucket3/*"));
        assertTrue(statement.getResources().contains("storage://bucket4/*"));

        // Test principals
        assertEquals(4, statement.getPrincipals().size());
        assertTrue(statement.getPrincipals().contains("principal1"));
        assertTrue(statement.getPrincipals().contains("principal2"));
        assertTrue(statement.getPrincipals().contains("principal3"));
        assertTrue(statement.getPrincipals().contains("principal4"));

        // Test conditions
        assertTrue(statement.getConditions().containsKey("StringEquals"));
        assertTrue(statement.getConditions().containsKey("DateGreaterThan"));
    }

    @Test
    public void testBuilderStateValidation() {
        // Test that trying to use statement methods without calling statement() throws exception
        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addAction("storage:GetObject").build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().effect("Allow").build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addResource("storage://test-bucket/*").build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addPrincipal("principal1").build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addCondition("StringEquals", "key", "value").build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addActions(Arrays.asList("storage:GetObject")).build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addResources(Arrays.asList("storage://test-bucket/*")).build());

        assertThrows(InvalidArgumentException.class, () ->
            PolicyDocument.builder().addPrincipals(Arrays.asList("principal1")).build());
    }

    @Test
    public void testAddNullStatement() {
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .addStatement(null)
            .statement("ValidStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        assertEquals(1, policy.getStatements().size());
        assertEquals("ValidStatement", policy.getStatements().get(0).getSid());
    }

    @Test
    public void testMixingAddStatementAndBuilder() {
        Statement preBuiltStatement = Statement.builder()
            .sid("PreBuilt")
            .effect("Deny")
            .addAction("storage:DeleteObject")
            .addResource("storage://sensitive-bucket/*")
            .build();

        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .addStatement(preBuiltStatement)
            .statement("BuiltInline")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://public-bucket/*")
            .endStatement()
            .build();

        assertEquals(2, policy.getStatements().size());
        assertEquals("PreBuilt", policy.getStatements().get(0).getSid());
        assertEquals("BuiltInline", policy.getStatements().get(1).getSid());
    }

    @Test
    public void testPolicyDocumentEqualsAndHashCode() {
        PolicyDocument policy1 = PolicyDocument.builder()
            .version("2024-01-01")
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        PolicyDocument policy2 = PolicyDocument.builder()
            .version("2024-01-01")
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        PolicyDocument policy3 = PolicyDocument.builder()
            .version("2023-01-01")
            .statement("DifferentStatement")
                .effect("Deny")
                .addAction("storage:DeleteObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        // Test equals
        assertEquals(policy1, policy2);
        assertNotEquals(policy1, policy3);
        assertNotEquals(policy1, null);
        assertNotEquals(policy1, "not a policy");
        assertEquals(policy1, policy1); // same object

        // Test hashCode
        assertEquals(policy1.hashCode(), policy2.hashCode());
        assertNotEquals(policy1.hashCode(), policy3.hashCode());
    }

    @Test
    public void testPolicyDocumentToString() {
        PolicyDocument policy = PolicyDocument.builder()
            .version("2024-01-01")
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        String result = policy.toString();
        assertTrue(result.contains("2024-01-01"));
        assertTrue(result.contains("TestStatement"));
        assertTrue(result.contains("PolicyDocument"));
    }


    @Test
    public void testEndStatementWithoutCurrentStatement() {
        // endStatement() should be safe to call even when no statement is being built
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .endStatement() // This should do nothing
            .statement("TestStatement")
                .effect("Allow")
                .addAction("storage:GetObject")
                .addResource("storage://test-bucket/*")
            .endStatement()
            .build();

        assertEquals(1, policy.getStatements().size());
    }
}