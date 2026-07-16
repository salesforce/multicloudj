package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * DocumentIterator backed by the Tablestore native GetRange API (see {@link GetRangeRunner}).
 *
 * <p>Drives cursor-based pagination: it fetches pages by following the server's
 * {@code nextStartPrimaryKey}, and the caller-facing {@link #getPaginationToken()} returns the
 * primary key of the LAST row handed out via {@link #next}. That is a positional bookmark a caller
 * can persist and use to resume in a later, separate query.
 *
 * <p>Resuming: the driver sets {@code inclusiveStartPrimaryKey} to the token's key. Because
 * Tablestore's range start is inclusive, the row equal to the resume key (the last one the previous
 * call already returned) is re-read and must be skipped; {@code resumeAfterKey} handles that.
 *
 * <p>Loop-accumulate: when a column filter is present, GetRange's {@code limit} caps rows SCANNED,
 * not RETURNED, so a page may yield fewer matches than requested (or none) while still returning a
 * cursor. {@link #hasNext} therefore keeps fetching until a row is available or the range is
 * exhausted.
 *
 * <p>Offset: GetRange has no native offset, so a caller-supplied offset is simulated by skipping
 * that many matching rows (fetching pages as needed) before the first row is returned. Offset and a
 * resume token are mutually exclusive at the query layer, so at most one applies.
 */
public class AliDocumentIterator implements DocumentIterator {

  private final GetRangeRunner runner;
  private final int limit;
  private final int offset;

  private final List<Row> buffer = new ArrayList<>();
  private int curr = 0;
  private int count = 0;
  // Number of leading matching rows still to skip to honor the query offset.
  private int toSkip;

  // Cursor to start the next fetch from; null means "use the runner's configured start bound".
  private PrimaryKey cursor;
  // When resuming from a caller token, the first row read equals this key and must be skipped.
  private PrimaryKey resumeAfterKey;
  private boolean firstFetch = true;
  // True once a fetch returns a null nextStartPrimaryKey (range exhausted).
  private boolean noMoreData = false;
  // Primary key of the most recently returned row; becomes the pagination token.
  private PrimaryKey lastConsumedKey;

  public AliDocumentIterator(
      GetRangeRunner runner, int offset, int limit, PrimaryKey resumeAfterKey) {
    this.runner = runner;
    this.offset = Math.max(offset, 0);
    this.toSkip = this.offset;
    this.limit = limit;
    this.resumeAfterKey = resumeAfterKey;
    this.cursor = resumeAfterKey; // null on a fresh query; the token key on resume
  }

  @Override
  public void next(Document document) {
    if (!hasNext()) {
      throw new NoSuchElementException("No more elements");
    }
    Row row = buffer.get(curr++);
    AliCodec.decodeDoc(row, document);
    lastConsumedKey = row.getPrimaryKey();
    count++;
  }

  @Override
  public boolean hasNext() {
    // Skip the offset's worth of leading matching rows before returning the first result.
    while (toSkip > 0) {
      if (!ensureRowAvailable()) {
        return false;
      }
      curr++;
      toSkip--;
    }
    if (limit > 0 && count >= limit) {
      return false;
    }
    return ensureRowAvailable();
  }

  // Ensures buffer.get(curr) is a real row, fetching pages as needed. False if range exhausted.
  //
  // A page can come back EMPTY yet non-final: when a column filter is set, GetRange's limit caps
  // rows SCANNED, not matched, so a page may filter out every scanned row while still returning a
  // live nextStartPrimaryKey. We must keep fetching across such empty pages until a row appears or
  // the cursor is exhausted (noMoreData). Gating the loop on "did this page yield a row" would stop
  // early on the first filtered-empty page and silently drop the matching rows on later pages.
  private boolean ensureRowAvailable() {
    while (curr >= buffer.size()) {
      if (noMoreData) {
        return false;
      }
      fetchNextPage();
    }
    return true;
  }

  // Fetches one more page into the buffer, following the cursor. The buffer may be left empty (a
  // filtered page); callers must consult noMoreData (not buffer emptiness) to decide if the range
  // is exhausted.
  private void fetchNextPage() {
    buffer.clear();
    curr = 0;
    PrimaryKey next = runner.run(cursor, buffer);

    // On the resume page, drop the leading row equal to the (inclusive) resume key.
    if (firstFetch
        && resumeAfterKey != null
        && !buffer.isEmpty()
        && buffer.get(0).getPrimaryKey().compareTo(resumeAfterKey) == 0) {
      buffer.remove(0);
    }
    firstFetch = false;

    if (next == null) {
      noMoreData = true;
    } else {
      cursor = next;
    }
  }

  @Override
  public void stop() {
    buffer.clear();
    cursor = null;
    noMoreData = true;
  }

  @Override
  public PaginationToken getPaginationToken() {
    // No rows consumed, or the range is fully drained: no resumable position.
    if (lastConsumedKey == null || (noMoreData && curr >= buffer.size())) {
      return new AliPaginationToken(null);
    }
    return new AliPaginationToken(lastConsumedKey);
  }
}
