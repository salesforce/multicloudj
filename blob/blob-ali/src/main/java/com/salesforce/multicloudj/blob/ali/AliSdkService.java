package com.salesforce.multicloudj.blob.ali;

import com.aliyun.sdk.service.oss2.exceptions.OperationException;
import com.aliyun.sdk.service.oss2.exceptions.ServiceException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.service.SdkService;

public interface AliSdkService extends SdkService {

  @Override
  default Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (Class<? extends SubstrateSdkException>) t.getClass();
    } else if (t instanceof OperationException) {
      Throwable cause = t.getCause();
      if (cause instanceof ServiceException) {
        String errorCode = ((ServiceException) cause).errorCode();
        return ErrorCodeMapping.getException(errorCode);
      }
      return UnknownException.class;
    } else if (t instanceof ServiceException) {
      String errorCode = ((ServiceException) t).errorCode();
      return ErrorCodeMapping.getException(errorCode);
    } else if (t instanceof IllegalArgumentException) {
      return InvalidArgumentException.class;
    }
    return UnknownException.class;
  }
}
