package com.salesforce.multicloudj.dbbackuprestore.aws;

import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.backup.model.BackupException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for AWS ErrorCodeMapping.
 */
public class ErrorCodeMappingTest {

    @Test
    void testAwsServiceException() {
        AwsServiceException exception = BackupException.builder()
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode("ResourceNotFoundException")
                        .errorMessage("Resource not found")
                        .build())
                .build();

        Class<? extends SubstrateSdkException> result =
                ErrorCodeMapping.getException(exception);
        assertEquals(ResourceNotFoundException.class, result);
    }
}
