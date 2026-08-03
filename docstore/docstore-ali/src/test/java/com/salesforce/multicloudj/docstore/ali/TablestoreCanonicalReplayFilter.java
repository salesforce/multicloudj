package com.salesforce.multicloudj.docstore.ali;

import com.github.tomakehurst.wiremock.extension.requestfilter.RequestFilterAction;
import com.github.tomakehurst.wiremock.extension.requestfilter.RequestWrapper;
import com.github.tomakehurst.wiremock.extension.requestfilter.StubRequestFilterV2;
import com.github.tomakehurst.wiremock.http.Body;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;

/**
 * Replay-time filter that rewrites an incoming Tablestore write request body (PutRow / DeleteRow)
 * to the order-independent canonical form produced by {@link TablestoreBodyCanonicalizer}, so it
 * matches the equivalently-canonicalized recorded stub (see {@link
 * TablestoreCanonicalRecordTransformer}) regardless of column serialization order.
 *
 * <p>No-op during recording (so the transformer sees the original bytes) and for any URL that is
 * not a canonicalizable Tablestore write.
 */
public class TablestoreCanonicalReplayFilter implements StubRequestFilterV2 {

  public static final String NAME = "tablestore-canonical-replay-filter";

  @Override
  public RequestFilterAction filter(Request request, ServeEvent serveEvent) {
    if (System.getProperty("record") != null) {
      return RequestFilterAction.continueWith(request);
    }
    if (!TablestoreBodyCanonicalizer.isCanonicalizableUrl(request.getUrl())) {
      return RequestFilterAction.continueWith(request);
    }
    byte[] canonical =
        TablestoreBodyCanonicalizer.canonicalize(request.getUrl(), request.getBody());
    Request wrapped =
        RequestWrapper.create().transformBody(body -> new Body(canonical)).wrap(request);
    return RequestFilterAction.continueWith(wrapped);
  }

  @Override
  public String getName() {
    return NAME;
  }
}
