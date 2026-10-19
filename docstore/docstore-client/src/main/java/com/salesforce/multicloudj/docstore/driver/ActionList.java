package com.salesforce.multicloudj.docstore.driver;

import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import lombok.Getter;

public class ActionList {

  @Getter private List<Action> actions;

  private boolean enableAtomicWrites;
  private final AbstractDocStore docStore;

  private Consumer<Predicate<Object>> beforeDo;

  public ActionList(AbstractDocStore docStore) {
    this.docStore = docStore;
    actions = new ArrayList<>();
  }

  public ActionList create(Document document) {
    actions.add(
        new Action(ActionKind.ACTION_KIND_CREATE, document, null, null, this.enableAtomicWrites));
    return this;
  }

  public ActionList replace(Document document) {
    actions.add(
        new Action(ActionKind.ACTION_KIND_REPLACE, document, null, null, this.enableAtomicWrites));
    return this;
  }

  public ActionList put(Document document) {
    actions.add(
        new Action(ActionKind.ACTION_KIND_PUT, document, null, null, this.enableAtomicWrites));
    return this;
  }

  public ActionList delete(Document document) {
    actions.add(
        new Action(ActionKind.ACTION_KIND_DELETE, document, null, null, this.enableAtomicWrites));
    return this;
  }

  public ActionList get(Document document, String... fieldPaths) {
    return get(document, null, fieldPaths);
  }

  /**
   * Adds a key read with options scoped to this action.
   *
   * @param document document with a valid key; receives the result
   * @param readOptions per-read options; null or unset values inherit the client setting
   * @param fieldPaths optional fields to retrieve
   * @return this action list
   */
  public ActionList get(Document document, ReadOptions readOptions, String... fieldPaths) {
    Action action =
        new Action(ActionKind.ACTION_KIND_GET, document, List.of(fieldPaths), null, false);
    action.setReadOptions(readOptions);
    actions.add(action);
    return this;
  }

  public ActionList update(Document document, Map<String, Object> mods) {
    actions.add(
        new Action(ActionKind.ACTION_KIND_UPDATE, document, null, mods, this.enableAtomicWrites));
    return this;
  }

  /**
   * Every write after this call will be executed in an atomic manner in a inAtomicWrite. Either
   * they all fail or succeed.
   */
  public ActionList enableAtomicWrites() {
    enableAtomicWrites = true;
    return this;
  }

  public ActionList beforeDo(Consumer<Predicate<Object>> beforeDo) {
    this.beforeDo = beforeDo;
    return this;
  }

  public void run() {
    try {
      docStore.checkClosed();
      toDriverActions();
      docStore.runActions(actions, beforeDo);
    } catch (Throwable e) {
      throw docStore.mapException(e);
    }
  }

  // Preprocess on the action list.
  // Use a set of (key + is Get action) for detecting duplicates: an action list can have at most
  // one get and at most one write for each key.
  private void toDriverActions() {
    Set<String> seenList = new HashSet<>();
    int index = 0;
    for (Action action : actions) {
      docStore.toDriverAction(action);
      if (action.getKey() != null) {
        String keyAndKind = action.getKey() + ":" + action.getKind();
        if (!seenList.contains(keyAndKind)) {
          seenList.add(keyAndKind);
        } else {
          throw new IllegalArgumentException("Duplicate key in action list: " + keyAndKind);
        }
      }
      action.setIndex(index);
      index++;
    }
  }
}
