package com.salesforce.multicloudj.blob.driver;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;

/** Wrapper object for directory upload data */
@Builder
@Getter
public class DirectoryUploadRequest {
  private final String localSourceDirectory;
  private final String prefix;
  private final boolean includeSubFolders;

  /**
   * When true, symbolic links encountered during directory traversal will be followed, uploading
   * the files they point to. Defaults to false.
   */
  private final boolean followSymbolicLinks;
  private final boolean transferStatusLoggingEnabled;

  /**
   * (Optional parameter) The map of tagName to tagValue to be associated with all blobs in the
   * directory
   */
  private final Map<String, String> tags;

  /**
   * Optional object lock (retention / legal hold) applied to each uploaded object, including the
   * directory marker when a prefix is used.
   */
  private final ObjectLockConfiguration objectLock;

  /**
   * (Optional parameter) The KMS key ID or ARN to use for server-side encryption of every object
   * uploaded from the directory. When set, the KMS key is applied to each object; when null,
   * either the bucket default or {@link #useKmsManagedKey} determines the encryption applied.
   */
  private final String kmsKeyId;

  /**
   * Set the serviceSideEncryption Header but don't set the kmsKeyId. When false and kmsKeyId is
   * null, no SSE headers are sent (bucket default applies). This option will trigger the use of the
   * cloud provider managed key. Providers whose native APIs do not accept a provider-managed-key
   * signal on write treat this field as a no-op.
   */
  private final boolean useKmsManagedKey;
}
