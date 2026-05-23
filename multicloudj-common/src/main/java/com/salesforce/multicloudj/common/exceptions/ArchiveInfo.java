package com.salesforce.multicloudj.common.exceptions;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class ArchiveInfo {
  private final boolean archived;
  private final String versionId;
}
