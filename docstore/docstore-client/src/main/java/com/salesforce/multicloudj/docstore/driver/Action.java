package com.salesforce.multicloudj.docstore.driver;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Action {

  private ActionKind kind; // the kind of action

  private Document document; // the document on which to perform the action

  protected boolean inAtomicWrite; // if the action is a part of atomic writes, only for writes

  // A list of field paths to retrieve, for Get only. Each field path is a dot separated string.
  private List<String> fieldPaths;

  // modifications to make, for Update only. The key is a field path (dot separated string).
  private Map<String, Object> mods;

  protected Object key; // the document key returned by Collection.Key, to avoid recomputing it

  protected int index = 0; // the index of the action in the original action list

  public Action(
      ActionKind kind,
      Document document,
      List<String> fieldPaths,
      Map<String, Object> mods,
      boolean inAtomicWrite) {
    this.kind = kind;
    this.document = document;
    this.fieldPaths = fieldPaths;
    this.mods = mods;
    this.inAtomicWrite = inAtomicWrite;
  }
}
