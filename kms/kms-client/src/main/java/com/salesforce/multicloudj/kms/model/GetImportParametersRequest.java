package com.salesforce.multicloudj.kms.model;

import lombok.Builder;
import lombok.Getter;

/** Request for the parameters needed to import key material into a key. */
@Getter
@Builder
public class GetImportParametersRequest {
  /** Key, created with {@link KeyOrigin#EXTERNAL}, that the material will be imported into. */
  private final KeyRef keyRef;

  @Builder.Default
  private final WrappingAlgorithm wrappingAlgorithm = WrappingAlgorithm.RSAES_OAEP_SHA_256;

  /**
   * Size of the wrapping public key. When unset the provider picks a size it supports; the size
   * that was used is reported in {@link ImportParameters#getWrappingKeySpec()}.
   */
  private final WrappingKeySpec wrappingKeySpec;
}
