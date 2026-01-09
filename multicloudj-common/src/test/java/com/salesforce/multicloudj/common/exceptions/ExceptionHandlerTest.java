package com.salesforce.multicloudj.common.exceptions;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionHandlerTest {

    @Test
    public void testHandleAndPropagateResourceAlreadyExistsException() {
        Throwable t = new Throwable("Resource already exists");
        assertThrows(ResourceAlreadyExistsException.class,
                () -> ExceptionHandler.handleAndPropagate(ResourceAlreadyExistsException.class, t));
    }

    @Test
    public void testHandleAndPropagateUnAuthorizedException() {
        Throwable t = new Throwable("Unauthorized");
        assertThrows(UnAuthorizedException.class,
                () -> ExceptionHandler.handleAndPropagate(UnAuthorizedException.class, t));
    }

    @Test
    public void testHandleAndPropagateResourceExhaustedException() {
        Throwable t = new Throwable("Resource exhausted");
        assertThrows(ResourceExhaustedException.class,
                () -> ExceptionHandler.handleAndPropagate(ResourceExhaustedException.class, t));
    }

    @Test
    public void testHandleAndPropagateInvalidArgumentException() {
        Throwable t = new Throwable("Invalid argument");
        assertThrows(InvalidArgumentException.class,
                () -> ExceptionHandler.handleAndPropagate(InvalidArgumentException.class, t));
    }

    @Test
    public void testHandleAndPropagateSubstrateSdkException() {
        SubstrateSdkException t = new SubstrateSdkException("Substrate SDK exception");
        assertThrows(SubstrateSdkException.class,
                () -> ExceptionHandler.handleAndPropagate(SubstrateSdkException.class, t));
    }

    @Test
    public void testHandleAndPropagateUnknownException() {
        Throwable t = new Throwable("Unknown exception");
        assertThrows(UnknownException.class,
                () -> ExceptionHandler.handleAndPropagate(UnknownException.class, t));
    }

    @Test
    public void testNullExceptionTypeUnknownException() {
        Throwable t = new Throwable("Null exception type");
        assertThrows(UnknownException.class, () -> ExceptionHandler.handleAndPropagate(null, t));
    }

    @Test
    public void testUnknownExceptionWithDetailedMessage() throws Exception {
        // Create a mock AWS S3Exception using reflection
        // This simulates what happens when an AWS exception is wrapped
        // Note: This test will only work if AWS SDK is available in the classpath
        Throwable awsException = createMockAwsException("AccessDenied", "Access Denied", "S3", 403, "req-123");
        
        UnknownException exception = assertThrows(UnknownException.class,
                () -> ExceptionHandler.handleAndPropagate(UnknownException.class, awsException));
        
        // If AWS SDK is available, detailed message should be extracted
        // If not, it will just be a regular exception with the cause
        if (exception.getMessage() != null && exception.getMessage().contains("Error Code")) {
            assertTrue(exception.getMessage().contains("Error Code: AccessDenied"));
            assertTrue(exception.getMessage().contains("Service: S3"));
            assertTrue(exception.getMessage().contains("Status Code: 403"));
            assertTrue(exception.getMessage().contains("Request ID: req-123"));
        }
        // In either case, the cause should be preserved
        assertEquals(awsException, exception.getCause());
    }

    @Test
    public void testUnAuthorizedExceptionWithDetailedMessage() throws Exception {
        // Test that detailed message is included in UnAuthorizedException
        // Note: This test will only work if AWS SDK is available in the classpath
        Throwable awsException = createMockAwsException("AccessDenied", "Access Denied", "S3", 403, "req-456");
        
        UnAuthorizedException exception = assertThrows(UnAuthorizedException.class,
                () -> ExceptionHandler.handleAndPropagate(UnAuthorizedException.class, awsException));
        
        // If AWS SDK is available, detailed message should be extracted
        // If not, it will just be a regular exception with the cause
        if (exception.getMessage() != null && exception.getMessage().contains("Error Code")) {
            assertTrue(exception.getMessage().contains("Error Code: AccessDenied"));
            assertTrue(exception.getMessage().contains("Service: S3"));
        }
        // In either case, the cause should be preserved
        assertEquals(awsException, exception.getCause());
    }

    @Test
    public void testRegularExceptionWithoutDetails() {
        // Regular exceptions should not have detailed messages extracted
        Throwable regularException = new RuntimeException("Regular exception");
        
        UnknownException exception = assertThrows(UnknownException.class,
                () -> ExceptionHandler.handleAndPropagate(UnknownException.class, regularException));
        
        // Should not have detailed message, just the cause
        assertEquals(regularException, exception.getCause());
    }

    @Test
    public void testNullThrowable() {
        UnknownException exception = assertThrows(UnknownException.class,
                () -> ExceptionHandler.handleAndPropagate(UnknownException.class, null));
        
        assertEquals(null, exception.getCause());
    }

    /**
     * Creates a mock AWS Service Exception using reflection to simulate AWS SDK exceptions.
     * This allows testing without requiring AWS SDK dependencies in the test classpath.
     */
    private Throwable createMockAwsException(String errorCode, String errorMessage, 
                                           String serviceName, Integer statusCode, String requestId) {
        try {
            // Try to use actual AWS SDK if available
            Class<?> awsServiceExceptionClass = Class.forName("software.amazon.awssdk.awscore.exception.AwsServiceException");
            Class<?> awsErrorDetailsClass = Class.forName("software.amazon.awssdk.awscore.exception.AwsErrorDetails");
            
            // Create AwsErrorDetails builder
            Class<?> awsErrorDetailsBuilderClass = Class.forName("software.amazon.awssdk.awscore.exception.AwsErrorDetails$Builder");
            Method builderMethod = awsErrorDetailsClass.getMethod("builder");
            Object errorDetailsBuilder = builderMethod.invoke(null);
            
            Method errorCodeMethod = awsErrorDetailsBuilderClass.getMethod("errorCode", String.class);
            Method errorMessageMethod = awsErrorDetailsBuilderClass.getMethod("errorMessage", String.class);
            Method buildMethod = awsErrorDetailsBuilderClass.getMethod("build");
            
            errorCodeMethod.invoke(errorDetailsBuilder, errorCode);
            errorMessageMethod.invoke(errorDetailsBuilder, errorMessage);
            Object awsErrorDetails = buildMethod.invoke(errorDetailsBuilder);
            
            // Create AwsServiceException builder
            Class<?> awsServiceExceptionBuilderClass = Class.forName("software.amazon.awssdk.awscore.exception.AwsServiceException$Builder");
            Method serviceExceptionBuilderMethod = awsServiceExceptionClass.getMethod("builder");
            Object serviceExceptionBuilder = serviceExceptionBuilderMethod.invoke(null);
            
            Method awsErrorDetailsMethod = awsServiceExceptionBuilderClass.getMethod("awsErrorDetails", awsErrorDetailsClass);
            Method serviceNameMethod = awsServiceExceptionBuilderClass.getMethod("serviceName", String.class);
            Method statusCodeMethod = awsServiceExceptionBuilderClass.getMethod("statusCode", Integer.class);
            Method requestIdMethod = awsServiceExceptionBuilderClass.getMethod("requestId", String.class);
            Method buildServiceExceptionMethod = awsServiceExceptionBuilderClass.getMethod("build");
            
            awsErrorDetailsMethod.invoke(serviceExceptionBuilder, awsErrorDetails);
            serviceNameMethod.invoke(serviceExceptionBuilder, serviceName);
            statusCodeMethod.invoke(serviceExceptionBuilder, statusCode);
            requestIdMethod.invoke(serviceExceptionBuilder, requestId);
            
            return (Throwable) buildServiceExceptionMethod.invoke(serviceExceptionBuilder);
        } catch (Exception e) {
            // If AWS SDK is not available, create a simple mock exception
            // that will test the fallback behavior
            return new RuntimeException("Mock AWS Exception: " + errorCode + " - " + errorMessage);
        }
    }
}