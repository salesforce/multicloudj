package com.salesforce.multicloudj.dbbackuprestore.aws;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.backup.model.BackupException;

/** Unit tests for AWS ErrorCodeMapping. */
public class ErrorCodeMappingTest {

  @Test
  void testAwsServiceException() {
    AwsServiceException exception =
        BackupException.builder()
            .awsErrorDetails(
                AwsErrorDetails.builder()
                    .errorCode("ResourceNotFoundException")
                    .errorMessage("Resource not found")
                    .build())
            .build();

    Class<? extends SubstrateSdkException> result = ErrorCodeMapping.getException(exception);
    assertEquals(ResourceNotFoundException.class, result);
  }
}
