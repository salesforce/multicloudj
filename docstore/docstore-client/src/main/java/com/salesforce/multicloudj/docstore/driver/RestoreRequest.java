package com.salesforce.multicloudj.docstore.driver;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Request object for restoring a backup to a document store collection/table.
 *
 * @since 0.2.26
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RestoreRequest {

    /**
     * ID of the backup to restore from.
     */
    private String backupId;

    /**
     * Name of the target collection/table to restore to.
     * If null or empty, restores to the original collection/table name.
     */
    private String targetCollectionName;

    /**
     * Provider-specific options as key-value pairs.
     * Can be used to pass additional cloud-specific parameters.
     *
     * <p>For AWS Docstore restore: must include {@code "iamRoleArn"} — the ARN of the IAM role
     * that AWS Backup assumes to perform the DynamoDB restore (e.g.
     * {@code "arn:aws:iam::123456789012:role/YourBackupRestoreRole"}).
     */
    private java.util.Map<String, String> options;
}
