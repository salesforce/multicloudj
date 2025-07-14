package com.salesforce.multicloudj.docstore.driver.codec;

import com.google.protobuf.Message;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.docstore.driver.FieldCache;
import com.salesforce.multicloudj.docstore.driver.FieldInfo;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Codec providers the utility for encoding and decoding the data between
 * java native types to the target types such as substrate specific types.
 */
public class Codec {
    /**
     * encode function encodes the Object v into the given encoder
     *
     * @param v   the Object to be encoded
     * @param enc the encoder used to encode the value.
     *            Every Encoder is supposed to hold the encoded value in their own way.
     *            For example: ListEncoder can have the List/Arrays and the
     *            MapEncoder can have the Map to save the encoded value
     * @throws IllegalArgumentException if something is off.
     */
    public static void encode(Object v, Encoder enc) throws IllegalArgumentException {
        if (v == null) {
            enc.encodeNil();
            return;
        }

        Class<?> clazz = v.getClass();
        if (v instanceof Message) {
            byte[] obj = ((Message) v).toByteArray();
            enc.encodeBytes(obj);
        } else if (v instanceof Boolean) {
            enc.encodeBool((Boolean) v);
        } else if (v instanceof Number) {
            if (v instanceof Integer) {
                enc.encodeInt(((Number) v).intValue());
            } else if (v instanceof Long) {
                enc.encodeLong(((Number) v).longValue());
            } else if (v instanceof Double || v instanceof Float) {
                enc.encodeFloat(((Number) v).doubleValue());
            }
        } else if (v instanceof String) {
            enc.encodeString((String) v);
        } else if (v instanceof byte[]) {
            enc.encodeBytes((byte[]) v);
        } else if (clazz.isArray()) {
            encodeArray(v, enc);
        } else if (v instanceof Collection) {
            encodeCollection((Collection<?>) v, enc);
        } else if (v instanceof Map) {
            encodeMap((Map<?, ?>) v, enc);
        } else if (clazz.isEnum()) {
            enc.encodeString(v.toString());
        } else {
            throw new IllegalArgumentException("Cannot encode type " + clazz.getName());
        }
    }

    /**
     * encodeList is responsible for encoding the array
     *
     * @param array holding the array object
     * @param enc   to be used for encoding the values
     */
    private static void encodeArray(Object array, Encoder enc) {
        int length = Array.getLength(array);
        Encoder listEncoder = enc.encodeList(length);

        for (int i = 0; i < length; i++) {
            Object item = Array.get(array, i);
            encode(item, listEncoder);
            listEncoder.listIndex(i);
        }
    }

    private static void encodeCollection(Collection<?> collection, Encoder enc) {
        Encoder listEncoder = enc.encodeList(collection.size());

        int index = 0;
        for (Object item : collection) {
            encode(item, listEncoder);
            listEncoder.listIndex(index++);
        }
    }

    public static void encodeMap(Map<?, ?> map, Encoder enc) {
        Encoder mapEncoder = enc.encodeMap(map.size());

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = stringifyMapKey(entry.getKey());
            Object value = entry.getValue();

            encode(value, mapEncoder);
            mapEncoder.mapKey(key);
        }
    }

    /**
     * Encodes the fields of the provided object of the user defined class using an encoder. This method uses reflection to access the object's
     * fields, encode their values, and write them to the {@code Encoder} in a map format.
     * <p>
     * The method retrieves the field information from the {@code FieldCache}, iterates through the fields, and encodes
     * each field value. If the field is a custom class, it recursively encodes the nested object. Primitive or simple
     * types are encoded directly. The field names are used as the map keys.
     *
     * @param obj   the object whose fields will be encoded. This object must not be {@code null}.
     * @param cache the {@code FieldCache} containing cached field information for the object's class. This cache provides
     *              the metadata needed to encode the fields. Must not be {@code null}.
     * @param enc   the {@code Encoder} responsible for encoding the object into a map. The encoder creates
     *              key-value pairs where field names are used as keys, and the encoded field values are used as the values.
     *              Must not be {@code null}.
     *
     * @throws NullPointerException if any of the arguments are {@code null}.
     * @throws RuntimeException if reflection fails during field access or if an object field is not accessible.
     */
    public static void encodeObject(Object obj, FieldCache cache, Encoder enc) {
        Collection<FieldInfo> fields = cache.getFields(obj.getClass());
        Encoder mapEncoder = enc.encodeMap(fields.size());
        for (FieldInfo fieldInfo : fields) {
            Field field = fieldInfo.getField();
            try {
                field.setAccessible(true);
                Object v = field.get(obj);
                if (v != null) {
                    if (isCustomClass(fieldInfo.getClazz())) {
                        encodeObject(v, cache, mapEncoder);
                    } else {
                        encode(v, mapEncoder);
                    }
                    mapEncoder.mapKey(fieldInfo.getName());
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Decodes the value in the decoder which is expected to have Map, updating the fields of the Object obj with the decoded values.
     * This method uses reflection to retrieve and decode fields dynamically based on the metadata cached in the
     * {@code FieldCache} and the map of key-value pairs decoded by the {@code Decoder}.
     * <p>
     * For each field, the decoder processes a key-value pair, finds the corresponding field by its name, and decodes the
     * value. If the field is a custom class, it recursively decodes its fields. Primitive or non-user defined types are decoded
     * directly.
     * <p>
     * This method also ensures that any {@code null} custom objects encountered during decoding are instantiated before
     * proceeding with recursive decoding.
     *
     * @param obj   the object where to put the decoded fields. This object must not be {@code null} but values don't matter.
     * @param cache the {@code FieldCache} containing cached fields information for the object's class. This cache provides
     *              the mapping of fields that need to be decoded. Must not be {@code null}.
     * @param dec   the {@code Decoder} responsible for decoding key-value pairs and passing them into the field updates.
     *              Must not be {@code null}.
     *
     * @throws NullPointerException if any of the arguments are {@code null}.
     * @throws InvalidArgumentException if a field corresponding to a key is not found in the class's metadata.
     * @throws RuntimeException if reflection fails during field access or instantiation (e.g., when creating a new instance
     *                          of a custom class).
     */
    public static void decodeToClass(Object obj, FieldCache cache, Decoder dec) {
        Objects.requireNonNull(obj, "Object cannot be null");
        Objects.requireNonNull(cache, "cache cannot be null");
        Objects.requireNonNull(dec, "decoder cannot be null");

        List<FieldInfo> fields = cache.getFields(obj.getClass());
        dec.decodeMap((key, vd) -> {
            FieldInfo fieldInfo = findFieldByName(fields, key);
            if (fieldInfo == null) {
                throw new InvalidArgumentException("Field " + key + " not found");
            }
            Field field = fieldInfo.getField();
            field.setAccessible(true);
            try {
                if (isCustomClass(fieldInfo.getClazz())) {
                    Object nestedObj = field.get(obj);
                    if (nestedObj == null) {
                        Class<?> clazz = fieldInfo.getClazz();
                        Constructor<?> constructor = clazz.getDeclaredConstructor();
                        constructor.setAccessible(true);
                        nestedObj = constructor.newInstance();
                    }
                    decodeToClass(nestedObj, cache, vd);
                    field.set(obj, nestedObj);
                } else {
                    Object v = field.get(obj);
                    v = decode(field.getType(), v, vd);
                    field.set(obj, v);
                }
            } catch (ReflectiveOperationException e) {
                throw new InvalidArgumentException("Exception occurred in the java reflections operation", e);
            }
            return true;
        });
    }

    private static FieldInfo findFieldByName(List<FieldInfo> fields, String name) {
        for (FieldInfo fieldInfo : fields) {
            if (fieldInfo.getName().equals(name)) {
                return fieldInfo;
            }
        }

        int index = Collections.binarySearch(fields, new FieldInfo(name, false, null, null), Comparator.comparing(FieldInfo::getName));
        if (index >= 0) {
            return fields.get(index);
        } else {
            return null;
        }
    }

    private static boolean isCustomClass(Class<?> clazz) {
        return !clazz.isPrimitive()
                && !Number.class.isAssignableFrom(clazz)
                && !Boolean.class.isAssignableFrom(clazz)
                && !String.class.isAssignableFrom(clazz)
                && !clazz.isArray()
                && !Collection.class.isAssignableFrom(clazz)
                && !Map.class.isAssignableFrom(clazz)
                && !clazz.getName().startsWith("java.")
                && !Message.class.isAssignableFrom(clazz);
    }

    public static String stringifyMapKey(Object key) throws IllegalArgumentException {
        if (key instanceof String) {
            return (String) key;
        }

        if (key instanceof Number) {
            return stringifyNumber((Number) key);
        }

        throw new IllegalArgumentException("Cannot encode key " + key + " of type " + key.getClass().getName());
    }

    private static String stringifyNumber(Number number) {
        if (number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long) {
            return Long.toString(number.longValue());
        } else if (number instanceof Float || number instanceof Double) {
            return Double.toString(number.doubleValue());
        } else {
            throw new IllegalArgumentException("Unsupported number type: " + number.getClass().getName());
        }
    }


    /**
     * decode function decodes the value in the decoder to target object.
     *
     * @param target  the class/type where to put the decoded result
     * @param v  the object where to put the decoded result
     * @param decoder the decoder used to decode the value.
     *                Every Decoder is supposed to hold the value to be decoded in their own way.
     *                For example: ListDecoder can have the List/Arrays and the
     *                MapDecoder can have the Map to save the value to be decoded.
     */
    public static Object decode(Class<?> target, Object v, Decoder decoder) {
        if (decoder.asNull()) {
            return null;
        }

        if (Message.class.isAssignableFrom(target)) {
            byte[] obj = decoder.asBytes();
            Method parseFromMethod;
            try {
                parseFromMethod = target.getMethod("parseFrom", byte[].class);
                return parseFromMethod.invoke(null, (Object) obj);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
        }

        if (Integer.class.isAssignableFrom(target) || int.class.isAssignableFrom(target)) {
            return decoder.asInt();
        }

        if (String.class.isAssignableFrom(target)) {
            return decoder.asString();
        }

        if (Float.class.isAssignableFrom(target) || float.class.isAssignableFrom(target)) {
            return decoder.asFloat();
        }

        if (Double.class.isAssignableFrom(target) || double.class.isAssignableFrom(target)) {
            return decoder.asDouble();
        }

        if (Boolean.class.isAssignableFrom(target) || boolean.class.isAssignableFrom(target)) {
            return decoder.asBool();
        }

        if (Long.class.isAssignableFrom(target) || long.class.isAssignableFrom(target)) {
            return decoder.asLong();
        }

        if (Map.class.isAssignableFrom(target)) {
            if (v == null) {
                v = new HashMap<String, Object>();
            }
            decodeMap((HashMap<String, Object>) v, decoder);
            return v;
        }

        if (target.isArray()) {
            return decodeArray(decoder);
        }

        if (Collection.class.isAssignableFrom(target)) {
            if (v == null) {
                v = new ArrayList<>();
            }
            decodeList((List<Object>) v, decoder);
            return v;
        }

        if (Object.class.isAssignableFrom(target)) {
            return decoder.asInterface();
        }
        return v;
    }

    /**
     * decodeMap decodes the value in decoder to the Map v
     *
     * @param v the target map object to hold the decoded value
     * @param d the decoder holding the initial value
     */
    public static void decodeMap(Map<String, Object> v, Decoder d) {
        d.decodeMap((key, vd) -> {
            Object el = new Object();
            try {
                el = decode(el.getClass(), el, vd);
            } catch (Exception e) {
                return false;
            }
            v.put(key, el);
            return true;
        });
    }

    /**
     * decodeList decodes the value in decoder to the List v
     *
     * @param v the target list object to hold the decoded value
     * @param d the decoder holding the initial value
     */
    private static void decodeList(List<Object> v, Decoder d) {
        d.decodeList((i, vd) -> {
            Object el = new Object();
            try {
                el = decode(el.getClass(), el, vd);
            } catch (Exception e) {
                return false;
            }
            v.add(i, el);
            return true;
        });
    }

    /**
     * decodeArray decodes the value in decoder to the List v
     *
     * @param d the decoder holding the initial value
     */
    private static Object[] decodeArray(Decoder d) {
        int n = d.listLen();
        Object[] v = new Object[n];
        d.decodeList((i, vd) -> {
            Object el = new Object();
            try {
                el = decode(el.getClass(), el, vd);
            } catch (Exception e) {
                return false;
            }
            v[i] = el;
            return true;
        });

        return v;
    }
}