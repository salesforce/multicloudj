package com.salesforce.multicloudj.docstore.ali;

import com.github.tomakehurst.wiremock.common.FileSource;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.StubMappingTransformer;
import com.github.tomakehurst.wiremock.matching.BinaryEqualToPattern;
import com.github.tomakehurst.wiremock.matching.ContentPattern;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import com.github.tomakehurst.wiremock.matching.RequestPattern;
import com.github.tomakehurst.wiremock.stubbing.StubMapping;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Record-time transformer that rewrites the body matcher of a recorded Tablestore write stub
 * (PutRow / DeleteRow) to the order-independent canonical form produced by {@link
 * TablestoreBodyCanonicalizer}.
 *
 * <p>Pairs with {@link TablestoreCanonicalReplayFilter}, which applies the same canonicalization to
 * the incoming request during replay. With both sides canonicalized, WireMock's ordinary {@code
 * binaryEqualTo} matches regardless of the column order the driver happened to serialize (which
 * varies run-to-run because the driver iterates a HashMap). Non-write stubs are left untouched.
 */
public class TablestoreCanonicalRecordTransformer extends StubMappingTransformer {

  public static final String NAME = "tablestore-canonical-record-transformer";

  @Override
  public StubMapping transform(StubMapping stubMapping, FileSource files, Parameters parameters) {
    RequestPattern request = stubMapping.getRequest();
    String url = request.getUrl();
    if (!TablestoreBodyCanonicalizer.isCanonicalizableUrl(url)) {
      return stubMapping;
    }
    List<ContentPattern<?>> bodyPatterns = request.getBodyPatterns();
    if (bodyPatterns == null || bodyPatterns.isEmpty()) {
      return stubMapping;
    }

    List<ContentPattern<?>> rewritten = new ArrayList<>();
    boolean changed = false;
    for (ContentPattern<?> pattern : bodyPatterns) {
      byte[] original = rawBytesOf(pattern);
      if (original != null) {
        byte[] canonical = TablestoreBodyCanonicalizer.canonicalize(url, original);
        rewritten.add(new BinaryEqualToPattern(canonical));
        changed = true;
      } else {
        rewritten.add(pattern);
      }
    }

    if (changed) {
      bodyPatterns.clear();
      bodyPatterns.addAll(rewritten);
    }
    return stubMapping;
  }

  /** Extracts the raw request bytes a body pattern was recorded from, or null if not applicable. */
  private static byte[] rawBytesOf(ContentPattern<?> pattern) {
    if (pattern instanceof BinaryEqualToPattern) {
      // getValue() returns the base64 string; getExpected() the decoded bytes are not exposed, so
      // decode the base64 ourselves.
      return java.util.Base64.getDecoder().decode(((BinaryEqualToPattern) pattern).getExpected());
    }
    if (pattern instanceof EqualToPattern) {
      // Text-recorded body (e.g. all-ASCII PlainBuffer). Reconstruct bytes 1:1 from ISO-8859-1 so
      // each char maps back to its original byte. (Lossy UTF-8 recordings won't round-trip, but the
      // lossless-recording factory stores non-text bodies as binaryEqualTo, so we only see text
      // here when the body genuinely was ASCII.)
      String expected = ((EqualToPattern) pattern).getExpected();
      return expected == null ? null : expected.getBytes(StandardCharsets.ISO_8859_1);
    }
    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean applyGlobally() {
    return false;
  }
}
