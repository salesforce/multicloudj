package com.salesforce.multicloudj.docstore.driver;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class FieldInfo {
  private String name;
  private boolean nameFromAnnotation = false;
  private Class<?> clazz;
  private Field field;

  public FieldInfo(Field field) {
    if (field.isAnnotationPresent(JsonProperty.class)) {
      this.name = field.getAnnotation(JsonProperty.class).value();
      this.nameFromAnnotation = true;
    } else {
      this.name = field.getName();
      this.nameFromAnnotation = false;
    }

    this.clazz = field.getType();
    this.field = field;
  }
}
