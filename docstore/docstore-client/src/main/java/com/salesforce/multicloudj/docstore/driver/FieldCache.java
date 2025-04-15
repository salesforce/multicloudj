package com.salesforce.multicloudj.docstore.driver;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.protobuf.Message;

public class FieldCache {

    private final Map<Class<?>, List<FieldInfo>> cacheMap = new ConcurrentHashMap<>();

    public FieldCache() {}

    // This method returns all the public and private fields of a class type, including the fields inherited.
    // Fields in the embedded class are ignored, such as static nested class, inner class, local class, and anonymous class.
    // Fields annotated with @JsonIgnore are ignored.
    // Field name is overridden by @JsonProperty annotation.
    public List<FieldInfo> getFields(Class<?> clazz) {
        if (!validateType(clazz)) {
            throw new IllegalArgumentException("Failed to get fields of type" + clazz.getName());
        }

        List<FieldInfo> fields = cacheMap.get(clazz);
        if (fields == null) {
            fields = listFields(clazz);
            fields.sort(Comparator.comparing(FieldInfo::getName));
            cacheMap.put(clazz, fields);
        }

        return fields;
    }

    private static boolean validateType(Class<?> clazz) {
        // The input type cannot be primitive, array, Collection, or Map.
        return !clazz.isPrimitive() && !clazz.isArray() && !Collection.class.isAssignableFrom(clazz) && !Map.class.isAssignableFrom(clazz) && !Message.class.isAssignableFrom(clazz);
    }

    private List<FieldInfo> listFields(Class<?> clazz) {
        List<FieldInfo> fields = new ArrayList<>();

        while (clazz != null) {
            Field[] allFields = clazz.getDeclaredFields();
            for (Field f : allFields) {
                if (isFieldIgnored(f)) {
                    continue;
                }

                FieldInfo fi = new FieldInfo(f);
                fields.add(fi);
            }
            // Move to the superclass
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    private boolean isFieldIgnored(Field field) {
        // Ignore the field if it is tagged @JsonIgnore
        return field.isAnnotationPresent(JsonIgnore.class) || field.isSynthetic();
    }

}
