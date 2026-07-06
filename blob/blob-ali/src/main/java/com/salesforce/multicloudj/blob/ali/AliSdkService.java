package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.exceptions.OperationException;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.salesforce.multicloudj.common.ali.AliRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.service.SdkService;

public interface AliSdkService extends SdkService {

  @Override
  default SubstrateSdkException mapException(Throwable t) {
    Throwable target = (t instanceof OperationException) ? t.getCause() : t;
    Class<? extends SubstrateSdkException> exceptionClass;
    Boolean retryableHint = null;
    if (target instanceof ServiceException) {
      ServiceException service = (ServiceException) target;
      exceptionClass = ErrorCodeMapping.getException(service.errorCode());
      retryableHint = AliRetryClassifier.classifyByStatusCode(service.statusCode());
    } else if (t instanceof IllegalArgumentException) {
      exceptionClass = InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    return ExceptionHandler.build(exceptionClass, t, retryableHint);
  }
}
