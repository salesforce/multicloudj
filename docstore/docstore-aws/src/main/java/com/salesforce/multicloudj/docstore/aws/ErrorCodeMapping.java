package com.salesforce.multicloudj.docstore.aws;

import com.google.common.collect.ImmutableMap;
import com.salesforce.multicloudj.common.aws.CommonErrorCodeMapping;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.ResourceConflictException;
import com.salesforce.multicloudj.common.exceptions.ResourceExhaustedException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.TransactionFailedException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;

import java.util.HashMap;
import java.util.Map;

/**
 * Provides the mapping of various error codes to SDK's exceptions
 */
public class ErrorCodeMapping {

    private ErrorCodeMapping() {
    }

    private static final Map<String, Class<? extends SubstrateSdkException>> ERROR_MAPPING;

    static {
        Map<String, Class<? extends SubstrateSdkException>> map = new HashMap<>(CommonErrorCodeMapping.get());
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Operations_Amazon_DynamoDB.html
        // BatchGetItem
        map.put("InternalServerError", UnknownException.class);
        map.put("ProvisionedThroughputExceededException", ResourceExhaustedException.class);
        map.put("RequestLimitExceeded", ResourceExhaustedException.class);
        map.put("ResourceNotFoundException", ResourceNotFoundException.class);
        // PutItem, DeleteItem
        map.put("ConditionalCheckFailedException", FailedPreconditionException.class);
        map.put("ItemCollectionSizeLimitExceededException", ResourceExhaustedException.class);
        map.put("TransactionConflictException", ResourceConflictException.class);
        // TransactWriteItems
        map.put("TransactionCanceledException", TransactionFailedException.class);
        ERROR_MAPPING = ImmutableMap.copyOf(map);
    }

    static Class<? extends SubstrateSdkException> getException(String errorCode) {
        return ERROR_MAPPING.getOrDefault(errorCode, UnknownException.class);
    }
}
