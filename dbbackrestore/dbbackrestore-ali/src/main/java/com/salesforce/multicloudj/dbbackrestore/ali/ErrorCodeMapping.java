package com.salesforce.multicloudj.dbbackrestore.ali;

import com.salesforce.multicloudj.common.exceptions.*;

/**
 * Maps Alibaba Cloud HBR exceptions to MultiCloudJ exception types.
 *
 * @since 0.2.25
 */
public class ErrorCodeMapping {

  /**
   * Maps an Alibaba Cloud exception to the appropriate MultiCloudJ exception type.
   * Error codes are defined here:
   * <a href="https://www.alibabacloud.com/help/en/cloud-backup/developer-reference/api-hbr-2017-09-08-errorcodes">...</a>
   * TODO: revisit this after conformance tests enabled to extract the error codes, there is a lack
   * of documentation which specific exception class is thrown by hbrclient.
   *
   * @param throwable the exception to map
   * @return the MultiCloudJ exception class
   */
  public static Class<? extends SubstrateSdkException> getException(Throwable throwable) {
    return UnknownException.class;
  }
}
