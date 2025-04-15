package com.salesforce.multicloudj.docstore.driver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

public class Util {
    private static final ObjectMapper mapper = new ObjectMapper();

    // GroupActions separates actions into four sets: writes, gets that must happen before the writes,
    // gets that must happen after the writes, and gets that can happen concurrently with the writes.
    public static void groupActions(List<Action> actions, List<Action> beforeGets,
                                    List<Action> getList, List<Action> writeList,
                                    List<Action> atomicWriteList, List<Action> afterGets) {
        Map<String, Action> bgets = new HashMap<>();
        Map<String, Action> agets = new HashMap<>();
        Map<String, Action> cgets = new HashMap<>();
        Map<String, Action> writes = new HashMap<>();
        Map<String, Action> atomicWrites = new HashMap<>();

        List<Action> nullKeys = new ArrayList<>();
        List<Action> atomicNullKeys = new ArrayList<>();

        for (Action action : actions) {
            String keyStr = serializeObject(action.getKey());
            if (action.getKey() == null) {
                handleNullKeyAction(action, nullKeys, atomicNullKeys);
            } else if (action.getKind() == ActionKind.ACTION_KIND_GET) {
                handleGetAction(action, keyStr, writes, atomicWrites, cgets, agets);
            } else {
                handleWriteAction(action, keyStr, cgets, bgets, writes, atomicWrites);
            }
        }

        beforeGets.addAll(bgets.values());
        beforeGets.sort(Comparator.comparing(Action::getIndex));
        getList.addAll(cgets.values());
        getList.sort(Comparator.comparing(Action::getIndex));
        writeList.addAll(writes.values());
        writeList.addAll(nullKeys);
        writeList.sort(Comparator.comparing(Action::getIndex));
        atomicWriteList.addAll(atomicWrites.values());
        atomicWriteList.sort(Comparator.comparing(Action::getIndex));
        afterGets.addAll(agets.values());
        afterGets.sort(Comparator.comparing(Action::getIndex));
    }

    private static void handleNullKeyAction(Action action, List<Action> nullKeys, List<Action> atomicNullKeys) {
        if (action.inAtomicWrite) {
            atomicNullKeys.add(action);
            return;
        }
        nullKeys.add(action);
    }

    private static void handleGetAction(Action action, String keyStr, Map<String, Action> writes,
                                        Map<String, Action> atomicWrites, Map<String, Action> cgets,
                                        Map<String, Action> agets) {
        if (writes.containsKey(keyStr) || atomicWrites.containsKey(keyStr)) {
            agets.put(keyStr, action);
        } else {
            cgets.put(keyStr, action);
        }
    }

    private static void handleWriteAction(Action action, String keyStr, Map<String, Action> cgets,
                                          Map<String, Action> bgets, Map<String, Action> writes,
                                          Map<String, Action> atomicWrites) {
        if (cgets.containsKey(keyStr)) {
            bgets.put(keyStr, cgets.remove(keyStr));
        }
        if (action.isInAtomicWrite()) {
            atomicWrites.put(keyStr, action);
        } else {
            writes.put(keyStr, action);
        }
    }

    // GroupByFieldPath collect the Get actions into groups with the same set of
    // field paths.
    public static List<List<Action>> groupByFieldPath(List<Action> gets) {
        List<List<Action>> groups = new ArrayList<>();
        Map<Action, Boolean> seen = new HashMap<>();
        while (seen.size() < gets.size()) {
            List<Action> g = new ArrayList<>();
            for (Action a : gets) {
                if (seen.get(a) == null && (g.isEmpty() || fpsEqual(g.get(0).getFieldPaths(), a.getFieldPaths()))) {
                    g.add(a);
                    seen.put(a, Boolean.TRUE);
                }

            }
            groups.add(g);
        }
        return groups;
    }

    // Compare to field paths
    private static boolean fpsEqual(List<String> fps1, List<String> fps2) {
        if (fps1.size() != fps2.size()) {
            return false;
        }

        for (int i = 0; i < fps1.size(); i++) {
            String fp1 = fps1.get(i);
            String fp2 = fps2.get(i);
            if (!fp1.equals(fp2)) {
                return false;
            }
        }
        return true;
    }

    public static String serializeObject(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize object", e);
        }
    }

    @Setter
    private static Supplier<String> uuidSupplier = () -> UUID.randomUUID().toString();

    public static String uniqueString() {
        return uuidSupplier.get();
    }

    public static void validateFieldPath(String fieldPath) {
        if (fieldPath == null || fieldPath.isEmpty()) {
            throw new IllegalArgumentException("fieldPath should not be null or empty");
        }

        // TODO: Validate field path is valid UTF-8 string.

        String[] parts = fieldPath.split("\\.");
        for (String part : parts) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException("fieldPath part should not be empty");
            }
        }
    }

}
