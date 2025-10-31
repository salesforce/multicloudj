package com.salesforce.multicloudj.iam.model;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
            .statement(Statement.builder()
                .sid("StorageAccess")
                .effect("Allow")
                .action("storage:GetObject")
                .action("storage:PutObject")
                .principal("arn:aws:iam::123456789012:user/ExampleUser")
                .resource("storage://my-bucket/*")
                .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
                .build())
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
            .statement(Statement.builder()
                .sid("ReadAccess")
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://my-bucket/*")
                .build())
            .statement(Statement.builder()
                .sid("WriteAccess")
                .effect("Allow")
                .action("storage:PutObject")
                .resource("storage://my-bucket/*")
                .build())
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
            .action("storage:GetObject")
            .resource("storage://my-bucket/*")
            .build();

        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement(statement)
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
                .statement(Statement.builder()
                    .sid("TestStatement")
                    .effect("Allow")
                    .action("storage:GetObject")
                    .resource("storage://test-bucket/*")
                    .build())
                .build();
        });
    }

    @Test
    public void testStatementWithoutEffectThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            PolicyDocument.builder()
                .version(TEST_VERSION)
                .statement(Statement.builder()
                    .sid("TestStatement")
                    .action("storage:GetObject")
                    .build())
                .build();
        });
    }

    @Test
    public void testStatementWithoutActionsThrowsException() {
        assertThrows(InvalidArgumentException.class, () -> {
            PolicyDocument.builder()
                .version(TEST_VERSION)
                .statement(Statement.builder()
                    .sid("TestStatement")
                    .effect("Allow")
                    .build())
                .build();
        });
    }

    @Test
    public void testVersionHandling() {
        // Test custom version
        PolicyDocument customVersionPolicy = PolicyDocument.builder()
            .version("2023-06-01")
            .statement(Statement.builder()
                .sid("TestStatement")
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .build())
            .build();

        assertEquals("2023-06-01", customVersionPolicy.getVersion());

        // Test default version
        PolicyDocument defaultVersionPolicy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement(Statement.builder()
                .sid("TestStatement")
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .build())
            .build();

        assertEquals(TEST_VERSION, defaultVersionPolicy.getVersion());
    }

    @Test
    public void testBuilderMethodsWithMultipleValues() {
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement(Statement.builder()
                .sid("TestStatement")
                .effect("Allow")
                .action("storage:GetObject")
                .action("storage:PutObject")
                .action("storage:DeleteObject")
                .action("storage:ListObjects")
                .resource("storage://bucket1/*")
                .resource("storage://bucket2/*")
                .resource("storage://bucket3/*")
                .resource("storage://bucket4/*")
                .principal("principal1")
                .principal("principal2")
                .principal("principal3")
                .principal("principal4")
                .condition("StringEquals", "aws:RequestedRegion", "us-west-2")
                .condition("DateGreaterThan", "aws:CurrentTime", "2024-01-01T00:00:00Z")
                .build())
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
    public void testBuilderAutoInitialization() {
        // Test simple statement creation with Lombok builder
        PolicyDocument policy1 = PolicyDocument.builder()
            .version("2024-01-01")
            .statement(Statement.builder()
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .build())
            .build();

        assertEquals(1, policy1.getStatements().size());
        assertEquals("Allow", policy1.getStatements().get(0).getEffect());

        PolicyDocument policy2 = PolicyDocument.builder()
            .version("2024-01-01")
            .statement(Statement.builder()
                .effect("Allow")
                .principal("principal1")
                .principal("principal2")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .condition("StringEquals", "key", "value")
                .build())
            .build();

        assertEquals(1, policy2.getStatements().size());
        Statement statement = policy2.getStatements().get(0);
        assertEquals("Allow", statement.getEffect());
        assertEquals(1, statement.getActions().size());
        assertEquals(1, statement.getResources().size());
        assertEquals(2, statement.getPrincipals().size());
    }

    @Test
    public void testAddNullStatement() {
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement((Statement) null)
            .statement(Statement.builder()
                .sid("ValidStatement")
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .build())
            .build();

        assertEquals(1, policy.getStatements().size()); // Null statements are filtered out
        assertEquals("ValidStatement", policy.getStatements().get(0).getSid());
    }

    @Test
    public void testMixingAddStatementAndBuilder() {
        Statement preBuiltStatement = Statement.builder()
            .sid("PreBuilt")
            .effect("Deny")
            .action("storage:DeleteObject")
            .resource("storage://sensitive-bucket/*")
            .build();

        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement(preBuiltStatement)
            .statement(Statement.builder()
                .sid("BuiltInline")
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://public-bucket/*")
                .build())
            .build();

        assertEquals(2, policy.getStatements().size());
        assertEquals("PreBuilt", policy.getStatements().get(0).getSid());
        assertEquals("BuiltInline", policy.getStatements().get(1).getSid());
    }

    @Test
    public void testEndStatementWithoutCurrentStatement() {
        // Simple test with Lombok builder
        PolicyDocument policy = PolicyDocument.builder()
            .version(TEST_VERSION)
            .statement(Statement.builder()
                .sid("TestStatement")
                .effect("Allow")
                .action("storage:GetObject")
                .resource("storage://test-bucket/*")
                .build())
            .build();

        assertEquals(1, policy.getStatements().size());
    }
}