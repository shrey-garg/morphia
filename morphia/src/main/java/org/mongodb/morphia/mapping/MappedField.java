/*
 * Copyright 2008-2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.morphia.mapping;


import com.mongodb.DBRef;
import org.bson.codecs.pojo.FieldModel;
import org.bson.codecs.pojo.TypeData;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.annotations.Serialized;
import org.mongodb.morphia.annotations.Transient;
import org.mongodb.morphia.annotations.Version;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;


/**
 * Represents the mapping of this field to/from mongodb (name, list<annotation>)
 */
@SuppressWarnings("unchecked")
public class MappedField {
    private final Map<Class<? extends Annotation>, Annotation> annotations;
    private MappedClass declaringClass;
    private FieldModel property; // the field :)

    MappedField(final MappedClass declaringClass, final FieldModel f) {
        this.declaringClass = declaringClass;
        property = f;

        final List<Annotation> list = property.getAnnotations();
        this.annotations = list.stream()
                               .map(ann -> this.<Class<? extends Annotation>, Annotation>map(ann.annotationType(), ann))
                               .reduce(new HashMap<>(), (map1, update) -> {
                                   map1.putAll(update);
                                   return map1;
                               });
    }

    private <K, V> Map<K, V> map(final K key, final V value) {
        final HashMap<K, V> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    /**
     * @return the declaring class of the java field
     */
    public Class getDeclaringClass() {
        return declaringClass.getClazz();
    }

    /**
     * Gets the value of the field mapped on the instance given.
     *
     * @param instance the instance to use
     * @return the value stored in the java field
     */
    public Object getFieldValue(final Object instance) {
        try {
            return property.getField().get(instance);
        } catch (IllegalAccessException e) {
            throw new MappingException(e.getMessage(), e);
        }
    }

    /**
     * @return the name of the java field, as declared on the class
     */
    public String getJavaFieldName() {
        return property.getName();
    }

    /**
     * If the java field is a list/array/map then the sub-type T is returned (ex. List<T>, T[], Map<?,T>
     *
     * @return the parameterized type of the field
     */
    public Class getSpecializedType() {
        Class specializedType;
        if (getType().isArray()) {
            specializedType = getType().getComponentType();
        } else {
            final List<TypeData<?>> typeParameters = property.getTypeData().getTypeParameters();
            specializedType = !typeParameters.isEmpty() ? typeParameters.get(0).getType() : null;
        }

        return specializedType;
    }

    /**
     * Indicates whether the annotation is present in the mapping (does not check the java field annotations, just the ones discovered)
     *
     * @param ann the annotation to search for
     * @return true if the annotation was found
     */
    public boolean hasAnnotation(final Class ann) {
        return annotations.containsKey(ann);
    }

    /**
     * @return the type of the underlying java field
     */
    public Class getType() {
        return property.getTypeData().getType();
    }

    /**
     * @return true if this field is marked as transient
     */
    public boolean isTransient() {
        return !hasAnnotation(Transient.class)
               && !hasAnnotation(java.beans.Transient.class)
               && Modifier.isTransient(getType().getModifiers());
    }

    /**
     * @return true if the MappedField is an array
     */
    public boolean isArray() {
        return getType().isArray();
    }

    /**
     * @return true if the MappedField is a Map
     */
    public boolean isMap() {
        return Map.class.isAssignableFrom(getTypeData().getType());
    }

    private boolean isCollection() {
        return Collection.class.isAssignableFrom(getTypeData().getType());
    }

    /**
     * @return true if this field is a container type such as a List, Map, Set, or array
     */
    public boolean isMultipleValues() {
        return !isScalarValue();
    }

    /**
     * @return true if this field is not a container type such as a List, Map, Set, or array
     */
    public boolean isScalarValue() {
        return !isMap() && !isArray() && !isCollection();
    }

    /**
     * @return true if this field is a reference to a foreign document
     * @see Reference
     * @see Key
     * @see DBRef
     */
    public boolean isReference() {
        return hasAnnotation(Reference.class) || Key.class == getConcreteType() || DBRef.class == getConcreteType();
    }

    /**
     * @return the concrete type of the MappedField
     */
    public Class getConcreteType() {
        final Property p = getAnnotation(Property.class);
        if (p != null) {
            final Class concrete = p.concreteClass();
            if (concrete != Object.class) {
                return concrete;
            }
        }
        return getType();
    }

    /**
     * @param clazz the annotation to search for
     * @param <T>   the type of the annotation
     * @return the annotation instance if it exists on this field
     */
    @SuppressWarnings("unchecked")
    public <T extends Annotation> T getAnnotation(final Class<T> clazz) {
        return (T) annotations.get(clazz);
    }

    /**
     * @return true if the MappedField is a Set
     */
    public boolean isSet() {
        return Set.class.isAssignableFrom(getTypeData().getType());
    }

    /**
     * Sets the value for the java field
     *
     * @param instance the instance to update
     * @param value    the value to set
     */
    public void setFieldValue(final Object instance, final Object value) {
        try {
            property.getField().set(instance, value);
        } catch (IllegalAccessException e) {
            throw new MappingException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return format("%s : %s", getNameToStore(), property.getTypeData().toString());
    }

    /**
     * @return the name of the field's (key)name for mongodb
     */
    public String getNameToStore() {
        return getMappedFieldName();
    }

    /**
     * @return the name of the field's key-name for mongodb
     */
    private String getMappedFieldName() {
        if (hasAnnotation(Id.class)) {
            return Mapper.ID_KEY;
        } else if (hasAnnotation(Property.class)) {
            final Property mv = (Property) annotations.get(Property.class);
            if (!mv.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return mv.value();
            }
        } else if (hasAnnotation(Reference.class)) {
            final Reference mr = (Reference) annotations.get(Reference.class);
            if (!mr.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return mr.value();
            }
        } else if (hasAnnotation(Embedded.class)) {
            final Embedded me = (Embedded) annotations.get(Embedded.class);
            if (!me.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return me.value();
            }
        } else if (hasAnnotation(Serialized.class)) {
            final Serialized me = (Serialized) annotations.get(Serialized.class);
            if (!me.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return me.value();
            }
        } else if (hasAnnotation(Version.class)) {
            final Version me = (Version) annotations.get(Version.class);
            if (!me.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return me.value();
            }
        }

        return property.getName();
    }

    public TypeData<?> getTypeData() {
        return property.getTypeData();
    }

    public boolean isParameterized() {
        return !getTypeData().getTypeParameters().isEmpty();
    }

    public Class getNormalizedType() {
        return !isParameterized() ? getTypeData().getType() : getTypeData().getTypeParameters().get(0).getType();
    }
}
