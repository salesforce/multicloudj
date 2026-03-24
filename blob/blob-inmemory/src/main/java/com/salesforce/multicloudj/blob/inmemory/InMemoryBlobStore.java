package com.salesforce.multicloudj.blob.inmemory;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import com.salesforce.multicloudj.blob.driver.AbstractBlobStore;
import com.salesforce.multicloudj.blob.driver.BlobIdentifier;
import com.salesforce.multicloudj.blob.driver.BlobInfo;
import com.salesforce.multicloudj.blob.driver.BlobMetadata;
import com.salesforce.multicloudj.blob.driver.ByteArray;
import com.salesforce.multicloudj.blob.driver.CopyFromRequest;
import com.salesforce.multicloudj.blob.driver.CopyRequest;
import com.salesforce.multicloudj.blob.driver.CopyResponse;
import com.salesforce.multicloudj.blob.driver.DownloadRequest;
import com.salesforce.multicloudj.blob.driver.DownloadResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageRequest;
import com.salesforce.multicloudj.blob.driver.ListBlobsPageResponse;
import com.salesforce.multicloudj.blob.driver.ListBlobsRequest;
import com.salesforce.multicloudj.blob.driver.MultipartPart;
import com.salesforce.multicloudj.blob.driver.MultipartUpload;
import com.salesforce.multicloudj.blob.driver.MultipartUploadRequest;
import com.salesforce.multicloudj.blob.driver.MultipartUploadResponse;
import com.salesforce.multicloudj.blob.driver.ObjectLockInfo;
import com.salesforce.multicloudj.blob.driver.PresignedUrlRequest;
import com.salesforce.multicloudj.blob.driver.UploadPartResponse;
import com.salesforce.multicloudj.blob.driver.UploadRequest;
import com.salesforce.multicloudj.blob.driver.UploadResponse;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;

/** InMemory implementation of BlobStore for testing purposes */
@AutoService(AbstractBlobStore.class)
public class InMemoryBlobStore extends AbstractBlobStore {

  private static final String PROVIDER_ID = "memory";

  // Shared storage across all instances - key is "bucket:key:versionId"
  private static final Map<String, StoredBlob> STORAGE = new ConcurrentHashMap<>();
  // Track latest version for each key - key is "bucket:key", value is versionId
  private static final Map<String, String> LATEST_VERSIONS = new ConcurrentHashMap<>();
  // Tags are per version - key is "bucket:key:versionId"
  private static final Map<String, Map<String, String>> TAGS = new ConcurrentHashMap<>();
  private static final Map<String, MultipartUploadState> MULTIPART_UPLOADS =
      new ConcurrentHashMap<>();
  // Track bucket metadata - key is bucket name
  static final Map<String, BucketMetadata> BUCKETS = new ConcurrentHashMap<>();

  public InMemoryBlobStore() {
    this(new Builder());
  }

  public InMemoryBlobStore(Builder builder) {
    super(builder);
    // Don't automatically register the bucket - it must be created explicitly
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  public Class<? extends SubstrateSdkException> getException(Throwable t) {
    if (t instanceof SubstrateSdkException) {
      return (Class<? extends SubstrateSdkException>) t.getClass();
    } else if (t instanceof IllegalArgumentException) {
      return InvalidArgumentException.class;
    }
    return UnknownException.class;
  }

  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, InputStream inputStream) {
    validateBucketExists();
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = inputStream.read(buffer)) != -1) {
        baos.write(buffer, 0, bytesRead);
      }
      return doUpload(uploadRequest, baos.toByteArray());
    } catch (Exception e) {
      throw new UnknownException("Failed to upload blob", e);
    }
  }

  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, byte[] content) {
    validateBucketExists();
    String baseKey = getStorageKey(uploadRequest.getKey());
    String etag = generateEtag(content);
    String versionId = UUID.randomUUID().toString();
    String versionedKey = baseKey + ":" + versionId;

    StoredBlob blob =
        new StoredBlob(
            content, etag, versionId, Instant.now(), uploadRequest.getMetadata(),
            uploadRequest.getContentType());

    STORAGE.put(versionedKey, blob);
    LATEST_VERSIONS.put(baseKey, versionId);

    // Store tags if provided
    if (uploadRequest.getTags() != null && !uploadRequest.getTags().isEmpty()) {
      TAGS.put(versionedKey, new HashMap<>(uploadRequest.getTags()));
    }

    return UploadResponse.builder()
        .key(uploadRequest.getKey())
        .versionId(versionId)
        .eTag(etag)
        .checksumValue(uploadRequest.getChecksumValue())
        .build();
  }

  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, File file) {
    try {
      byte[] content = Files.readAllBytes(file.toPath());
      return doUpload(uploadRequest, content);
    } catch (Exception e) {
      throw new UnknownException("Failed to upload blob from file", e);
    }
  }

  @Override
  protected UploadResponse doUpload(UploadRequest uploadRequest, Path path) {
    try {
      byte[] content = Files.readAllBytes(path);
      return doUpload(uploadRequest, content);
    } catch (Exception e) {
      throw new UnknownException("Failed to upload blob from path", e);
    }
  }

  @Override
  protected DownloadResponse doDownload(
      DownloadRequest downloadRequest, OutputStream outputStream) {
    validateBucketExists();
    String baseKey = getStorageKey(downloadRequest.getKey());
    String versionId = downloadRequest.getVersionId();

    // If no version specified, get the latest version
    if (versionId == null) {
      versionId = LATEST_VERSIONS.get(baseKey);
    }

    if (versionId == null) {
      throw new ResourceNotFoundException("Blob not found: " + downloadRequest.getKey());
    }

    String versionedKey = baseKey + ":" + versionId;
    StoredBlob blob = STORAGE.get(versionedKey);

    if (blob == null) {
      throw new ResourceNotFoundException(
          "Blob version not found: " + downloadRequest.getKey() + " version: " + versionId);
    }

    try {
      byte[] data =
          extractRange(blob.getData(), downloadRequest.getStart(), downloadRequest.getEnd());
      outputStream.write(data);
      return buildDownloadResponse(downloadRequest.getKey(), blob, data.length);
    } catch (Exception e) {
      throw new UnknownException("Failed to download blob", e);
    }
  }

  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, ByteArray byteArray) {
    validateBucketExists();
    String baseKey = getStorageKey(downloadRequest.getKey());
    String versionId = downloadRequest.getVersionId();

    // If no version specified, get the latest version
    if (versionId == null) {
      versionId = LATEST_VERSIONS.get(baseKey);
    }

    if (versionId == null) {
      throw new ResourceNotFoundException("Blob not found: " + downloadRequest.getKey());
    }

    String versionedKey = baseKey + ":" + versionId;
    StoredBlob blob = STORAGE.get(versionedKey);

    if (blob == null) {
      throw new ResourceNotFoundException(
          "Blob version not found: " + downloadRequest.getKey() + " version: " + versionId);
    }

    byte[] data =
        extractRange(blob.getData(), downloadRequest.getStart(), downloadRequest.getEnd());
    byteArray.setBytes(data);
    return buildDownloadResponse(downloadRequest.getKey(), blob, data.length);
  }

  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, File file) {
    try {
      return doDownload(downloadRequest, file.toPath());
    } catch (Exception e) {
      throw new UnknownException("Failed to download blob to file", e);
    }
  }

  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest, Path path) {
    validateBucketExists();
    String baseKey = getStorageKey(downloadRequest.getKey());
    String versionId = downloadRequest.getVersionId();

    // If no version specified, get the latest version
    if (versionId == null) {
      versionId = LATEST_VERSIONS.get(baseKey);
    }

    if (versionId == null) {
      throw new ResourceNotFoundException("Blob not found: " + downloadRequest.getKey());
    }

    String versionedKey = baseKey + ":" + versionId;
    StoredBlob blob = STORAGE.get(versionedKey);

    if (blob == null) {
      throw new ResourceNotFoundException(
          "Blob version not found: " + downloadRequest.getKey() + " version: " + versionId);
    }

    try {
      byte[] data =
          extractRange(blob.getData(), downloadRequest.getStart(), downloadRequest.getEnd());
      Files.write(path, data);
      return buildDownloadResponse(downloadRequest.getKey(), blob, data.length);
    } catch (Exception e) {
      throw new UnknownException("Failed to download blob to path", e);
    }
  }

  @Override
  protected DownloadResponse doDownload(DownloadRequest downloadRequest) {
    validateBucketExists();
    String baseKey = getStorageKey(downloadRequest.getKey());
    String versionId = downloadRequest.getVersionId();

    // If no version specified, get the latest version
    if (versionId == null) {
      versionId = LATEST_VERSIONS.get(baseKey);
    }

    if (versionId == null) {
      throw new ResourceNotFoundException("Blob not found: " + downloadRequest.getKey());
    }

    String versionedKey = baseKey + ":" + versionId;
    StoredBlob blob = STORAGE.get(versionedKey);

    if (blob == null) {
      throw new ResourceNotFoundException(
          "Blob version not found: " + downloadRequest.getKey() + " version: " + versionId);
    }

    byte[] data =
        extractRange(blob.getData(), downloadRequest.getStart(), downloadRequest.getEnd());
    InputStream inputStream = new ByteArrayInputStream(data);
    return buildDownloadResponse(downloadRequest.getKey(), blob, data.length, inputStream);
  }

  @Override
  protected void doDelete(String key, String versionId) {
    validateBucketExists();
    String baseKey = getStorageKey(key);

    // If version ID is provided, delete only that version
    if (versionId != null) {
      String versionedKey = baseKey + ":" + versionId;
      STORAGE.remove(versionedKey);
      TAGS.remove(versionedKey);

      // If deleting the latest version, clear the latest version tracker
      String latestVersion = LATEST_VERSIONS.get(baseKey);
      if (versionId.equals(latestVersion)) {
        LATEST_VERSIONS.remove(baseKey);
      }
    } else {
      // Delete all versions of this key
      String latestVersion = LATEST_VERSIONS.get(baseKey);
      if (latestVersion != null) {
        String versionedKey = baseKey + ":" + latestVersion;
        STORAGE.remove(versionedKey);
        TAGS.remove(versionedKey);
        LATEST_VERSIONS.remove(baseKey);
      }

      // Also delete any other versions
      List<String> keysToDelete = new ArrayList<>();
      for (String storageKey : STORAGE.keySet()) {
        if (storageKey.startsWith(baseKey + ":")) {
          keysToDelete.add(storageKey);
        }
      }
      for (String storageKey : keysToDelete) {
        STORAGE.remove(storageKey);
        TAGS.remove(storageKey);
      }
    }
  }

  @Override
  protected void doDelete(Collection<BlobIdentifier> objects) {
    for (BlobIdentifier obj : objects) {
      doDelete(obj.getKey(), obj.getVersionId());
    }
  }

  @Override
  protected CopyResponse doCopy(CopyRequest request) {
    validateBucketExists();
    // Also validate destination bucket exists
    if (!BUCKETS.containsKey(request.getDestBucket())) {
      throw new ResourceNotFoundException(
          "Destination bucket not found: " + request.getDestBucket());
    }

    String srcBaseKey = getStorageKey(request.getSrcKey());
    String srcVersionId = request.getSrcVersionId();

    // If no version specified, get the latest version
    if (srcVersionId == null) {
      srcVersionId = LATEST_VERSIONS.get(srcBaseKey);
    }

    if (srcVersionId == null) {
      throw new ResourceNotFoundException("Source blob not found: " + request.getSrcKey());
    }

    String srcVersionedKey = srcBaseKey + ":" + srcVersionId;
    StoredBlob sourceBlob = STORAGE.get(srcVersionedKey);

    if (sourceBlob == null) {
      throw new ResourceNotFoundException("Source blob not found: " + request.getSrcKey());
    }

    // Copy to destination (could be different bucket)
    String destBaseKey = request.getDestBucket() + ":" + request.getDestKey();
    String newVersionId = UUID.randomUUID().toString();
    String destVersionedKey = destBaseKey + ":" + newVersionId;
    StoredBlob destBlob =
        new StoredBlob(
            sourceBlob.getData().clone(),
            sourceBlob.getEtag(),
            newVersionId,
            Instant.now(),
            sourceBlob.getMetadata(),
            sourceBlob.getContentType());

    STORAGE.put(destVersionedKey, destBlob);
    LATEST_VERSIONS.put(destBaseKey, newVersionId);

    return CopyResponse.builder()
        .key(request.getDestKey())
        .versionId(newVersionId)
        .eTag(destBlob.getEtag())
        .lastModified(destBlob.getLastModified())
        .build();
  }

  @Override
  protected CopyResponse doCopyFrom(CopyFromRequest request) {
    validateBucketExists();

    String srcBaseKey = request.getSrcBucket() + ":" + request.getSrcKey();
    String srcVersionId = request.getSrcVersionId();

    // If no version specified, get the latest version
    if (srcVersionId == null) {
      srcVersionId = LATEST_VERSIONS.get(srcBaseKey);
    }

    if (srcVersionId == null) {
      throw new ResourceNotFoundException(
          "Source blob not found: " + request.getSrcBucket() + "/" + request.getSrcKey());
    }

    String srcVersionedKey = srcBaseKey + ":" + srcVersionId;
    StoredBlob sourceBlob = STORAGE.get(srcVersionedKey);

    if (sourceBlob == null) {
      throw new ResourceNotFoundException(
          "Source blob not found: " + request.getSrcBucket() + "/" + request.getSrcKey());
    }

    String destBaseKey = getStorageKey(request.getDestKey());
    String newVersionId = UUID.randomUUID().toString();
    String destVersionedKey = destBaseKey + ":" + newVersionId;
    StoredBlob destBlob =
        new StoredBlob(
            sourceBlob.getData().clone(),
            sourceBlob.getEtag(),
            newVersionId,
            Instant.now(),
            sourceBlob.getMetadata(),
            sourceBlob.getContentType());

    STORAGE.put(destVersionedKey, destBlob);
    LATEST_VERSIONS.put(destBaseKey, newVersionId);

    return CopyResponse.builder()
        .key(request.getDestKey())
        .versionId(newVersionId)
        .eTag(destBlob.getEtag())
        .lastModified(destBlob.getLastModified())
        .build();
  }

  @Override
  protected BlobMetadata doGetMetadata(String key, String versionId) {
    validateBucketExists();
    String baseKey = getStorageKey(key);

    // If no version specified, get the latest version
    if (versionId == null) {
      versionId = LATEST_VERSIONS.get(baseKey);
    }

    if (versionId == null) {
      throw new ResourceNotFoundException("Blob not found: " + key);
    }

    String versionedKey = baseKey + ":" + versionId;
    StoredBlob blob = STORAGE.get(versionedKey);

    if (blob == null) {
      throw new ResourceNotFoundException(
          "Blob version not found: " + key + " version: " + versionId);
    }

    return BlobMetadata.builder()
        .key(key)
        .versionId(blob.getVersionId())
        .eTag(blob.getEtag())
        .objectSize((long) blob.getData().length)
        .metadata(blob.getMetadata())
        .lastModified(blob.getLastModified())
        .contentType(blob.getContentType())
        .build();
  }

  @Override
  protected Iterator<BlobInfo> doList(ListBlobsRequest request) {
    validateBucketExists();
    String prefix = request.getPrefix() != null ? request.getPrefix() : "";
    String delimiter = request.getDelimiter();

    // List only latest versions
    List<BlobInfo> blobs =
        LATEST_VERSIONS.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(bucket + ":"))
            .filter(
                entry -> {
                  String key = entry.getKey().substring((bucket + ":").length());
                  if (!key.startsWith(prefix)) {
                    return false;
                  }
                  // If delimiter is specified, filter out keys containing the delimiter after the
                  // prefix
                  if (delimiter != null && !delimiter.isEmpty()) {
                    String keyAfterPrefix = key.substring(prefix.length());
                    return !keyAfterPrefix.contains(delimiter);
                  }
                  return true;
                })
            .map(
                entry -> {
                  String key = entry.getKey().substring((bucket + ":").length());
                  String versionId = entry.getValue();
                  String versionedKey = entry.getKey() + ":" + versionId;
                  StoredBlob blob = STORAGE.get(versionedKey);
                  if (blob == null) {
                    return null;
                  }
                  return new BlobInfo.Builder()
                      .withKey(key)
                      .withObjectSize((long) blob.getData().length)
                      .withLastModified(blob.getLastModified())
                      .build();
                })
            .filter(blobInfo -> blobInfo != null)
            .sorted(Comparator.comparing(BlobInfo::getKey))
            .collect(Collectors.toList());

    return blobs.iterator();
  }

  @Override
  protected ListBlobsPageResponse doListPage(ListBlobsPageRequest request) {
    validateBucketExists();
    String prefix = request.getPrefix() != null ? request.getPrefix() : "";
    String delimiter = request.getDelimiter();
    int maxKeys = request.getMaxResults() != null ? request.getMaxResults() : 1000;
    String continuationToken = request.getPaginationToken();

    // Step 1: collect all matching blob keys -> StoredBlob from latest versions
    TreeMap<String, StoredBlob> matchingBlobs = new TreeMap<>();
    for (Map.Entry<String, String> entry : LATEST_VERSIONS.entrySet()) {
      if (!entry.getKey().startsWith(bucket + ":")) {
        continue;
      }
      String key = entry.getKey().substring((bucket + ":").length());
      if (!key.startsWith(prefix)) {
        continue;
      }
      String versionedKey = entry.getKey() + ":" + entry.getValue();
      StoredBlob blob = STORAGE.get(versionedKey);
      if (blob != null) {
        matchingBlobs.put(key, blob);
      }
    }

    // Step 2: build a unified sorted entry list.
    // Each entry is (entryString -> isCommonPrefix).
    // For keys that fold under a delimiter: entryString is the common prefix string.
    // For direct blob keys: entryString is the key itself.
    // TreeMap keeps lexicographic order; common prefixes are deduplicated.
    TreeMap<String, Boolean> allEntries = new TreeMap<>();
    Set<String> seenPrefixes = new HashSet<>();

    for (String key : matchingBlobs.keySet()) {
      if (delimiter != null && !delimiter.isEmpty()) {
        String keyAfterPrefix = key.substring(prefix.length());
        int delimIdx = keyAfterPrefix.indexOf(delimiter);
        if (delimIdx >= 0) {
          // Key folds into a common prefix entry
          String commonPrefix =
              prefix + keyAfterPrefix.substring(0, delimIdx + delimiter.length());
          if (seenPrefixes.add(commonPrefix)) {
            allEntries.put(commonPrefix, true);
          }
          continue;
        }
      }
      // Direct blob entry
      allEntries.put(key, false);
    }

    // Step 3: apply continuation token — skip all entries up to and including the token
    List<Map.Entry<String, Boolean>> sortedEntries = new ArrayList<>(allEntries.entrySet());
    int startIndex = 0;
    if (continuationToken != null) {
      for (int i = 0; i < sortedEntries.size(); i++) {
        if (sortedEntries.get(i).getKey().equals(continuationToken)) {
          startIndex = i + 1;
          break;
        }
      }
    }

    // Step 4: apply combined maxKeys budget across both blobs and common prefixes
    int endIndex = Math.min(startIndex + maxKeys, sortedEntries.size());
    List<Map.Entry<String, Boolean>> pageEntries = sortedEntries.subList(startIndex, endIndex);

    // Step 5: split page entries into blobs and common prefixes
    List<BlobInfo> blobs = new ArrayList<>();
    List<String> commonPrefixes = new ArrayList<>();
    for (Map.Entry<String, Boolean> entry : pageEntries) {
      if (entry.getValue()) {
        commonPrefixes.add(entry.getKey());
      } else {
        StoredBlob stored = matchingBlobs.get(entry.getKey());
        blobs.add(
            new BlobInfo.Builder()
                .withKey(entry.getKey())
                .withObjectSize((long) stored.getData().length)
                .withLastModified(stored.getLastModified())
                .build());
      }
    }

    boolean isTruncated = endIndex < sortedEntries.size();
    String nextToken =
        isTruncated ? pageEntries.get(pageEntries.size() - 1).getKey() : null;

    return new ListBlobsPageResponse(blobs, commonPrefixes, isTruncated, nextToken);
  }

  @Override
  protected MultipartUpload doInitiateMultipartUpload(MultipartUploadRequest request) {
    validateBucketExists();
    String uploadId = UUID.randomUUID().toString();

    MultipartUploadState state =
        new MultipartUploadState(request.getKey(), request.getMetadata(), request.getContentType());

    MULTIPART_UPLOADS.put(uploadId, state);

    return MultipartUpload.builder()
        .id(uploadId)
        .bucket(bucket)
        .key(request.getKey())
        .metadata(request.getMetadata())
        .tags(request.getTags())
        .checksumEnabled(request.isChecksumEnabled())
        .kmsKeyId(request.getKmsKeyId())
        .build();
  }

  @Override
  protected UploadPartResponse doUploadMultipartPart(MultipartUpload mpu, MultipartPart mpp) {
    MultipartUploadState state = MULTIPART_UPLOADS.get(mpu.getId());

    if (state == null) {
      throw new ResourceNotFoundException("Multipart upload not found: " + mpu.getId());
    }

    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = mpp.getInputStream().read(buffer)) != -1) {
        baos.write(buffer, 0, bytesRead);
      }

      byte[] partData = baos.toByteArray();
      String etag = generateEtag(partData);
      String checksumValue = computeCrc32cChecksum(partData);

      state.addPart(mpp.getPartNumber(), partData, etag);

      return new UploadPartResponse(mpp.getPartNumber(), etag, partData.length, checksumValue);
    } catch (Exception e) {
      throw new UnknownException("Failed to upload multipart part", e);
    }
  }

  @Override
  protected MultipartUploadResponse doCompleteMultipartUpload(
      MultipartUpload mpu, List<UploadPartResponse> parts) {
    MultipartUploadState state = MULTIPART_UPLOADS.get(mpu.getId());

    if (state == null) {
      throw new ResourceNotFoundException("Multipart upload not found: " + mpu.getId());
    }

    // Validate all parts exist and ETags match
    for (UploadPartResponse part : parts) {
      PartData partData = state.getParts().get(part.getPartNumber());
      if (partData == null) {
        throw new ResourceNotFoundException("Part not found: " + part.getPartNumber());
      }
      // Validate ETag matches
      if (!partData.getEtag().equals(part.getEtag())) {
        throw new com.salesforce.multicloudj.common.exceptions.InvalidArgumentException(
            "ETag mismatch for part "
                + part.getPartNumber()
                + ": expected "
                + partData.getEtag()
                + ", got "
                + part.getEtag());
      }
    }

    // Combine all parts in order
    ByteArrayOutputStream combined = new ByteArrayOutputStream();
    try {
      for (UploadPartResponse part :
          parts.stream()
              .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
              .collect(Collectors.toList())) {
        byte[] partDataBytes = state.getPart(part.getPartNumber());
        if (partDataBytes == null) {
          throw new ResourceNotFoundException("Part not found: " + part.getPartNumber());
        }
        combined.write(partDataBytes);
      }

      byte[] finalData = combined.toByteArray();
      String etag = generateEtag(finalData);
      String versionId = UUID.randomUUID().toString();

      String baseKey = getStorageKey(mpu.getKey());
      String versionedKey = baseKey + ":" + versionId;
      StoredBlob blob =
          new StoredBlob(
              finalData, etag, versionId, Instant.now(), state.getMetadata(),
              state.getContentType());

      STORAGE.put(versionedKey, blob);
      LATEST_VERSIONS.put(baseKey, versionId);
      MULTIPART_UPLOADS.remove(mpu.getId());
      String checksumValue = computeCrc32cChecksum(finalData);

      return new MultipartUploadResponse(etag, checksumValue);
    } catch (Exception e) {
      throw new UnknownException("Failed to complete multipart upload", e);
    }
  }

  @Override
  protected List<UploadPartResponse> doListMultipartUpload(MultipartUpload mpu) {
    MultipartUploadState state = MULTIPART_UPLOADS.get(mpu.getId());

    if (state == null) {
      throw new ResourceNotFoundException("Multipart upload not found: " + mpu.getId());
    }

    return state.getParts().entrySet().stream()
        .map(
            entry ->
                new UploadPartResponse(
                    entry.getKey(), entry.getValue().getEtag(), entry.getValue().getData().length))
        .sorted(Comparator.comparingInt(UploadPartResponse::getPartNumber))
        .collect(Collectors.toList());
  }

  @Override
  protected void doAbortMultipartUpload(MultipartUpload mpu) {
    validateBucketExists();
    MULTIPART_UPLOADS.remove(mpu.getId());
  }

  @Override
  protected Map<String, String> doGetTags(String key) {
    validateBucketExists();
    String baseKey = getStorageKey(key);
    String versionId = LATEST_VERSIONS.get(baseKey);

    if (versionId == null) {
      return new HashMap<>();
    }

    String versionedKey = baseKey + ":" + versionId;
    Map<String, String> tags = TAGS.get(versionedKey);
    return tags != null ? new HashMap<>(tags) : new HashMap<>();
  }

  @Override
  protected void doSetTags(String key, Map<String, String> tags) {
    validateBucketExists();
    String baseKey = getStorageKey(key);
    String versionId = LATEST_VERSIONS.get(baseKey);

    if (versionId == null) {
      throw new ResourceNotFoundException("Blob not found: " + key);
    }

    String versionedKey = baseKey + ":" + versionId;
    TAGS.put(versionedKey, new HashMap<>(tags));
  }

  @Override
  protected URL doGeneratePresignedUrl(PresignedUrlRequest request) {
    // Don't validate bucket existence - presigned URLs are client-side operations
    try {
      // For in-memory implementation, just return a fake URL
      return new URL("http://localhost:8080/" + bucket + "/" + request.getKey());
    } catch (MalformedURLException e) {
      throw new UnknownException("Failed to generate presigned URL", e);
    }
  }

  @Override
  protected boolean doDoesObjectExist(String key, String versionId) {
    validateBucketExists();
    String baseKey = getStorageKey(key);

    // If version ID is provided, check for that specific version
    if (versionId != null) {
      String versionedKey = baseKey + ":" + versionId;
      return STORAGE.containsKey(versionedKey);
    }

    // Otherwise, check if any version exists (check latest version tracker)
    return LATEST_VERSIONS.containsKey(baseKey);
  }

  @Override
  protected boolean doDoesBucketExist() {
    return BUCKETS.containsKey(bucket);
  }

  @Override
  public void close() {
    // Nothing to close for in-memory implementation
  }

  // Helper methods

  private void validateBucketExists() {
    if (!BUCKETS.containsKey(bucket)) {
      throw new ResourceNotFoundException("Bucket not found: " + bucket);
    }
  }

  private String getStorageKey(String key) {
    return bucket + ":" + key;
  }

  private String generateEtag(byte[] data) {
    return "\"" + Integer.toHexString(java.util.Arrays.hashCode(data)) + "\"";
  }

  private String computeCrc32cChecksum(byte[] data) {
    java.util.zip.CRC32C crc32c = new java.util.zip.CRC32C();
    crc32c.update(data);
    long value = crc32c.getValue();
    byte[] checksumBytes = new byte[4];
    checksumBytes[0] = (byte) (value >> 24);
    checksumBytes[1] = (byte) (value >> 16);
    checksumBytes[2] = (byte) (value >> 8);
    checksumBytes[3] = (byte) value;
    return java.util.Base64.getEncoder().encodeToString(checksumBytes);
  }

  private byte[] extractRange(byte[] data, Long start, Long end) {
    int dataLength = data.length;

    // No range specified, return entire data
    if (start == null && end == null) {
      return data;
    }

    int startPos;
    int endPos;

    if (start == null) {
      // Last 'end' bytes - if end exceeds data length, return entire data
      int requestedEnd = end.intValue();
      if (requestedEnd >= dataLength) {
        return data;
      }
      startPos = Math.max(0, dataLength - requestedEnd);
      endPos = dataLength;
    } else if (end == null) {
      // From 'start' to end
      startPos = start.intValue();
      // Validate that start is within bounds
      if (startPos >= dataLength) {
        throw new ResourceNotFoundException(
            "Requested range not satisfiable: start="
                + start
                + " exceeds data length="
                + dataLength);
      }
      endPos = dataLength;
    } else {
      // From 'start' to 'end' (inclusive)
      startPos = start.intValue();
      // If end exceeds data length, adjust to data length
      endPos = Math.min(end.intValue() + 1, dataLength);

      // Validate that start is within bounds
      if (startPos >= dataLength) {
        throw new ResourceNotFoundException(
            "Requested range not satisfiable: start="
                + start
                + " exceeds data length="
                + dataLength);
      }
    }

    // Validate range is valid
    if (startPos < 0 || startPos >= endPos) {
      return new byte[0];
    }

    byte[] result = new byte[endPos - startPos];
    System.arraycopy(data, startPos, result, 0, endPos - startPos);
    return result;
  }

  private DownloadResponse buildDownloadResponse(String key, StoredBlob blob, int contentLength) {
    return DownloadResponse.builder()
        .key(key)
        .metadata(
            BlobMetadata.builder()
                .key(key)
                .versionId(blob.getVersionId())
                .eTag(blob.getEtag())
                .objectSize((long) contentLength)
                .metadata(blob.getMetadata())
                .lastModified(blob.getLastModified())
                .contentType(blob.getContentType())
                .build())
        .build();
  }

  private DownloadResponse buildDownloadResponse(
      String key, StoredBlob blob, int contentLength, InputStream inputStream) {
    return DownloadResponse.builder()
        .key(key)
        .metadata(
            BlobMetadata.builder()
                .key(key)
                .versionId(blob.getVersionId())
                .eTag(blob.getEtag())
                .objectSize((long) contentLength)
                .metadata(blob.getMetadata())
                .lastModified(blob.getLastModified())
                .contentType(blob.getContentType())
                .build())
        .inputStream(inputStream)
        .build();
  }

  @Override
  public ObjectLockInfo getObjectLock(String key, String versionId) {
    return null;
  }

  @Override
  public void updateObjectRetention(String key, String versionId, Instant retainUntilDate) {}

  @Override
  public void updateLegalHold(String key, String versionId, boolean legalHold) {}

  // Inner classes for storage

  @Getter
  static class BucketMetadata {
    private final Instant creationDate;

    public BucketMetadata(Instant creationDate) {
      this.creationDate = creationDate;
    }
  }

  @Getter
  private static class StoredBlob {
    private final byte[] data;
    private final String etag;
    private final String versionId;
    private final Instant lastModified;
    private final Map<String, String> metadata;
    private final String contentType;

    public StoredBlob(
        byte[] data,
        String etag,
        String versionId,
        Instant lastModified,
        Map<String, String> metadata,
        String contentType) {
      this.data = data;
      this.etag = etag;
      this.versionId = versionId;
      this.lastModified = lastModified;
      this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
      this.contentType = contentType;
    }
  }

  @Getter
  private static class PartData {
    private final byte[] data;
    private final String etag;

    public PartData(byte[] data, String etag) {
      this.data = data;
      this.etag = etag;
    }
  }

  @Getter
  private static class MultipartUploadState {
    private final String key;
    private final Map<String, String> metadata;
    private final String contentType;
    private final Map<Integer, PartData> parts = new ConcurrentHashMap<>();

    public MultipartUploadState(String key, Map<String, String> metadata, String contentType) {
      this.key = key;
      this.metadata = metadata;
      this.contentType = contentType;
    }

    public void addPart(int partNumber, byte[] data, String etag) {
      parts.put(partNumber, new PartData(data, etag));
    }

    public byte[] getPart(int partNumber) {
      PartData part = parts.get(partNumber);
      return part != null ? part.getData() : null;
    }
  }

  // Builder

  @Getter
  public static class Builder extends AbstractBlobStore.Builder<InMemoryBlobStore, Builder> {

    public Builder() {
      providerId(PROVIDER_ID);
    }

    @Override
    public Builder self() {
      return this;
    }

    @Override
    public InMemoryBlobStore build() {
      return new InMemoryBlobStore(this);
    }
  }

  // Public methods for testing

  /**
   * Creates a bucket for testing purposes.
   *
   * @param bucketName the name of the bucket to create
   */
  public static void createBucket(String bucketName) {
    BUCKETS.putIfAbsent(bucketName, new BucketMetadata(Instant.now()));
  }

  /** Clears all in-memory storage including buckets, blobs, tags, and multipart uploads. */
  public static void clearStorage() {
    STORAGE.clear();
    LATEST_VERSIONS.clear();
    TAGS.clear();
    MULTIPART_UPLOADS.clear();
    BUCKETS.clear();
  }
}
