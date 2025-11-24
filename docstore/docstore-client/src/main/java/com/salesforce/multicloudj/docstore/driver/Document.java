package com.salesforce.multicloudj.docstore.driver;

import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import com.salesforce.multicloudj.docstore.driver.codec.Decoder;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
The Document class is a wrapper around the Map or the POJO defined by end user.
End users should not directly call the methods of the Document class, they should interact with the underlying POJO or Map that the Document wraps.
 */
public class Document {

    private Map<String, Object> m = null;
    private List<FieldInfo> fieldList = null;   // Sorted by the field name.
    private Object doc = null;    // Store the copy of the origin object and all changes.

    private FieldCache fieldCache = null;

    public Document(Object s) {
        if (s == null) {
            throw new IllegalArgumentException("Document input cannot be null.");
        }

        if (s instanceof Map<?, ?>) {
            this.m = (Map<String, Object>)s;
        } else {
            this.doc = s;
            this.fieldCache = new FieldCache();
            this.fieldList = fieldCache.getFields(s.getClass());
        }
    }

    private FieldInfo getFieldInfo(String fieldName) {
        if (m != null) {
            return null;    // This method only applies to class object.
        }
        int index = Collections.binarySearch(fieldList, new FieldInfo(fieldName, false, null, null), Comparator.comparing(FieldInfo::getName));
        if (index >= 0) {
            return fieldList.get(index);
        } else {
            return null;
        }
    }

    public Object getField(String fieldName) {
        if (m != null) {
            return m.get(fieldName);
        } else {
            FieldInfo fi = getFieldInfo(fieldName);
            if (fi != null) {
                fi.getField().setAccessible(true);
                try {
                    return fi.getField().get(doc);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to get a field " + fi.getName(), e);
                }
            }
            return null;
        }
    }

    public void setField(String fieldName, Object value) {
        if (m != null) {
            m.put(fieldName, value);
            return;
        }
        FieldInfo fi = getFieldInfo(fieldName);
        if (fi != null) {
            fi.getField().setAccessible(true);
            try {
                fi.getField().set(doc, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to set a field " + fi.getName(), e);
            }
        } else {
            throw new IllegalArgumentException("Field " + fieldName + " does not exist.");
        }
    }

    // getDocument gets the value of the given field path, which must be a document.
    // If create is true, it creates intermediate documents as needed.
    public Document getDocument(String[] fp, boolean create) {
        if (fp.length == 0) {
            return this;
        }

        Object x = getField(fp[0]);
        if (x == null) {
            if (create) {
                x = new HashMap<String, Object>();
                setField(fp[0], x);
            } else {
                return null;
            }
        }

        Document d = new Document(x);
        return d.getDocument(Arrays.copyOfRange(fp, 1, fp.length), create);
    }

    public Object get(String[] fp) {
        Document d = getDocument(Arrays.copyOfRange(fp, 0, fp.length - 1), false);
        if (d != null) {
            return d.getField(fp[fp.length - 1]);
        }
        return null;
    }

    public void set(String[] fp, Object value) {
        Document d = getDocument(Arrays.copyOfRange(fp, 0, fp.length - 1), true);
        if (d != null) {
            d.setField(fp[fp.length - 1], value);
        }
    }

    public List<String> getFieldNames() {
        ArrayList<String> fieldNames = new ArrayList<>();
        if (m != null) {
            for (Map.Entry<String, Object> entry : m.entrySet()) {
                fieldNames.add(entry.getKey());
            }
        } else {
            for (FieldInfo fi : fieldList) {
                fieldNames.add(fi.getName());
            }
        }
        return fieldNames;
    }

    public void encode(Encoder enc) {
        if (m != null) {
            Codec.encodeMap(m, enc);
        } else {
            Codec.encodeObject(doc, fieldCache, enc);
        }
    }

    public void decode(Decoder dec) {
        if (m != null) {
            Codec.decodeMap(m, dec);
        } else {
            Codec.decodeToClass(doc, fieldCache, dec);
        }
    }

    public boolean hasField(String fieldName) {
        if (m != null) {
            return m.containsKey(fieldName);
        } else {
            FieldInfo fi = getFieldInfo(fieldName);
            return fi != null;
        }
    }

}
