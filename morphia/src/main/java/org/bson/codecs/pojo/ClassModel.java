/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.codecs.pojo;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;

/**
 * This model represents the metadata for a class and all its properties.
 *
 * @param <T> The type of the class the ClassModel represents
 * @since 3.5
 */
public final class ClassModel<T> {
    private final String name;
    private final Class<T> type;
    private final boolean hasTypeParameters;
    private final InstanceCreatorFactory<T> instanceCreatorFactory;
    private final boolean discriminatorEnabled;
    private final String discriminatorKey;
    private final String discriminator;
    private final PropertyModel<?> idProperty;
    private final List<Annotation> annotations;
    private final List<PropertyModel<?>> propertyModels;
    private final List<FieldModel<?>> fieldModels;
    private final Map<String, TypeParameterMap> propertyNameToTypeParameterMap;

    ClassModel(final Class<T> clazz, final Map<String, TypeParameterMap> propertyNameToTypeParameterMap,
               final InstanceCreatorFactory<T> instanceCreatorFactory, final Boolean discriminatorEnabled, final String discriminatorKey,
               final String discriminator, final PropertyModel<?> idProperty, final List<Annotation> annotations,
               final List<FieldModel<?>> fieldModels, final List<PropertyModel<?>> propertyModels) {
        this.name = clazz.getSimpleName();
        this.type = clazz;
        this.hasTypeParameters = clazz.getTypeParameters().length > 0;
        this.propertyNameToTypeParameterMap = propertyNameToTypeParameterMap;
        this.instanceCreatorFactory = instanceCreatorFactory;
        this.discriminatorEnabled = discriminatorEnabled;
        this.discriminatorKey = discriminatorKey;
        this.discriminator = discriminator;
        this.idProperty = idProperty;
        this.propertyModels = propertyModels;
        this.annotations = annotations;
        this.fieldModels = fieldModels;
    }

    /**
     * Creates a new Class Model builder instance using reflection.
     *
     * @param type the POJO class to reflect and configure the builder with.
     * @param <S> the type of the class
     * @return a new Class Model builder instance using reflection on the {@code clazz}.
     */
    public static <S> ClassModelBuilder<S> builder(final Class<S> type) {
        return new ClassModelBuilder<S>(type);
    }

    /**
     * @return a new InstanceCreator instance for the ClassModel
     */
    public InstanceCreator<T> getInstanceCreator() {
        return instanceCreatorFactory.create();
    }

    /**
     * @return the backing class for the ClassModel
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * @return true if the underlying type has type parameters.
     */
    public boolean hasTypeParameters() {
        return hasTypeParameters;
    }

    /**
     * @return true if a discriminator should be used when storing the data.
     */
    public boolean useDiscriminator() {
        return discriminatorEnabled;
    }

    /**
     * Gets the value for the discriminator.
     *
     * @return the discriminator value or null if not set
     */
    public String getDiscriminatorKey() {
        return discriminatorKey;
    }

    /**
     * Returns the discriminator key.
     *
     * @return the discriminator key or null if not set
     */
    public String getDiscriminator() {
        return discriminator;
    }

    /**
     * Gets a {@link PropertyModel} by the property name.
     *
     * @param propertyName the PropertyModel's property name
     * @return the PropertyModel or null if the property is not found
     */
    public PropertyModel<?> getPropertyModel(final String propertyName) {
        for (PropertyModel<?> propertyModel : propertyModels) {
            if (propertyModel.getName().equals(propertyName)) {
                return propertyModel;
            }
        }
        return null;
    }

    /**
     * Gets a {@link FieldModel} by the field name.
     *
     * @param fieldName the FieldModel's field name
     * @return the FieldModel or null if the field is not found
     */
    public FieldModel<?> getFieldModel(final String fieldName) {
        for (FieldModel<?> fieldModel : fieldModels) {
            if (fieldModel.getName().equals(fieldName)) {
                return fieldModel;
            }
        }
        return null;
    }

    /**
     * Returns all the annotations on this model
     *
     * @return the list of annotations
     */
    public List<Annotation> getAnnotations() {
        return annotations;
    }

    /**
     * Returns all the fields on this model
     *
     * @return the list of fields
     */
    public List<FieldModel<?>> getFieldModels() {
        return fieldModels;
    }

    /**
     * Returns all the properties on this model
     *
     * @return the list of properties
     */
    public List<PropertyModel<?>> getPropertyModels() {
        return propertyModels;
    }

    /**
     * Returns the {@link PropertyModel} mapped as the id property for this ClassModel
     *
     * @return the PropertyModel for the id
     */
    public PropertyModel<?> getIdPropertyModel() {
        return idProperty;
    }

    /**
     * Returns the name of the class represented by this ClassModel
     *
     * @return the name
     */
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClassModel)) {
            return false;
        }

        final ClassModel<?> that = (ClassModel<?>) o;

        if (hasTypeParameters != that.hasTypeParameters) {
            return false;
        }
        if (discriminatorEnabled != that.discriminatorEnabled) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        if (instanceCreatorFactory != null ? !instanceCreatorFactory.equals(that.instanceCreatorFactory)
                                           : that.instanceCreatorFactory != null) {
            return false;
        }
        if (discriminatorKey != null ? !discriminatorKey.equals(that.discriminatorKey) : that.discriminatorKey != null) {
            return false;
        }
        if (discriminator != null ? !discriminator.equals(that.discriminator) : that.discriminator != null) {
            return false;
        }
        if (idProperty != null ? !idProperty.equals(that.idProperty) : that.idProperty != null) {
            return false;
        }
        if (propertyModels != null ? !propertyModels.equals(that.propertyModels) : that.propertyModels != null) {
            return false;
        }
        if (fieldModels != null ? !fieldModels.equals(that.fieldModels) : that.fieldModels != null) {
            return false;
        }
        if (annotations != null ? !annotations.equals(that.annotations) : that.annotations != null) {
            return false;
        }
        return propertyNameToTypeParameterMap != null ? propertyNameToTypeParameterMap.equals(that.propertyNameToTypeParameterMap)
                                                      : that.propertyNameToTypeParameterMap == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (hasTypeParameters ? 1 : 0);
        result = 31 * result + (instanceCreatorFactory != null ? instanceCreatorFactory.hashCode() : 0);
        result = 31 * result + (discriminatorEnabled ? 1 : 0);
        result = 31 * result + (discriminatorKey != null ? discriminatorKey.hashCode() : 0);
        result = 31 * result + (discriminator != null ? discriminator.hashCode() : 0);
        result = 31 * result + (idProperty != null ? idProperty.hashCode() : 0);
        result = 31 * result + (propertyModels != null ? propertyModels.hashCode() : 0);
        result = 31 * result + (fieldModels != null ? fieldModels.hashCode() : 0);
        result = 31 * result + (propertyNameToTypeParameterMap != null ? propertyNameToTypeParameterMap.hashCode() : 0);
        return result;
    }

    InstanceCreatorFactory<T> getInstanceCreatorFactory() {
        return instanceCreatorFactory;
    }

    Map<String, TypeParameterMap> getPropertyNameToTypeParameterMap() {
        return propertyNameToTypeParameterMap;
    }

    @Override
       public String toString() {
           return "ClassModel{" +
                  "name='" + name + '\'' +
                  ", type=" + type +
                  ", hasTypeParameters=" + hasTypeParameters +
                  ", discriminatorEnabled=" + discriminatorEnabled +
                  ", discriminatorKey='" + discriminatorKey + '\'' +
                  ", discriminator='" + discriminator + '\'' +
                  ", idProperty=" + idProperty +
                  ", propertyModels=" + propertyModels +
                  ", propertyNameToTypeParameterMap=" + propertyNameToTypeParameterMap +
                  '}';
       }
}
