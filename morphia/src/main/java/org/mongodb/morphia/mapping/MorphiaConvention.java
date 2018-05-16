package org.mongodb.morphia.mapping;

import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.FieldModelBuilder;
import org.bson.codecs.pojo.PropertyAccessor;
import org.bson.codecs.pojo.PropertyMetadata;
import org.bson.codecs.pojo.PropertyModelBuilder;
import org.bson.codecs.pojo.PropertySerialization;
import org.bson.codecs.pojo.TypeData;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.annotations.Serialized;
import org.mongodb.morphia.annotations.Transient;
import org.mongodb.morphia.annotations.Version;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.reflect.Modifier.isAbstract;
import static org.bson.codecs.pojo.PojoBuilderHelper.createPropertyModelBuilder;

@SuppressWarnings("unchecked")
public class MorphiaConvention implements Convention {
    private MapperOptions options;

    MorphiaConvention(final MapperOptions options) {
        this.options = options;
    }

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        classModelBuilder.discriminator(classModelBuilder.getType().getName())
                         .discriminatorKey("className");

        classModelBuilder.enableDiscriminator(!classModelBuilder.hasAnnotation(Entity.class)
                                              || !classModelBuilder.getAnnotation(Entity.class).noClassnameStored()
                                              || isNotConcrete(classModelBuilder.getType()));

        final List<String> names = classModelBuilder.getPropertyModelBuilders().stream()
                                                    .map(PropertyModelBuilder::getName)
                                                    .collect(Collectors.toList());
        for (final String name : names) {
            classModelBuilder.removeProperty(name);
        }

        Iterator<FieldModelBuilder<?>> iterator = classModelBuilder.getFieldModelBuilders().iterator();
        while (iterator.hasNext()) {
            final FieldModelBuilder<?> builder = iterator.next();
            final Field field = builder.getField();

            PropertyModelBuilder<?> property = classModelBuilder.getProperty(builder.getName());

            if (Modifier.isStatic(field.getModifiers()) || isTransient(builder)) {
                iterator.remove();
                if (property != null) {
                    classModelBuilder.removeProperty(property.getName());
                }
            } else {
                if (property == null) {
                    final PropertyMetadata<?> propertyMetadata = new PropertyMetadata<>(builder.getName(),
                        classModelBuilder.getType().getName(), builder.getTypeData())
                                                                     .field(field);
                    property = createPropertyModelBuilder(propertyMetadata);
                    classModelBuilder.addProperty(property);
                }
                property.typeData((TypeData) builder.getTypeData());
                if (builder.hasAnnotation(Id.class)) {
                    classModelBuilder.idPropertyName(property.getReadName());
                }

                final String mappedName = getMappedFieldName(builder);
                property.readName(mappedName)
                        .writeName(mappedName)
                        .propertyAccessor(field.getType().isArray()
                                          ? new ArrayFieldAccessor(field)
                                          : new FieldAccessor(field))
                .propertySerialization(new MorphiaPropertySerialization(options, builder));

                final Class<?> type = property.getTypeData().getType();
                if (Collection.class.isAssignableFrom(type)
                    || Map.class.isAssignableFrom(type)) {
                    property.discriminatorEnabled(true);
                }

                if (isNotConcrete(type)) {
                    property.discriminatorEnabled(true);
                }
            }
        }

    }

    private boolean isNotConcrete(final Class<?> type) {
        return type.isInterface() || isAbstract(type.getModifiers());
    }

    private static boolean isTransient(final FieldModelBuilder<?> field) {
        return field.hasAnnotation(Transient.class)
               || field.hasAnnotation(java.beans.Transient.class)
               || Modifier.isTransient(field.getTypeData().getType().getModifiers());
    }

    private static String getMappedFieldName(FieldModelBuilder<?> field) {
        if (field.hasAnnotation(Id.class)) {
            return Mapper.ID_KEY;
        } else if (field.hasAnnotation(Property.class)) {
            final Property mv = field.getAnnotation(Property.class);
            if (!mv.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return mv.value();
            }
        } else if (field.hasAnnotation(Reference.class)) {
            final Reference mr = field.getAnnotation(Reference.class);
            if (!mr.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return mr.value();
            }
        } else if (field.hasAnnotation(Embedded.class)) {
            final Embedded me = field.getAnnotation(Embedded.class);
            if (!me.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return me.value();
            }
        } else if (field.hasAnnotation(Serialized.class)) {
            final Serialized me = field.getAnnotation(Serialized.class);
            if (!me.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return me.value();
            }
        } else if (field.hasAnnotation(Version.class)) {
            final Version me = field.getAnnotation(Version.class);
            if (!me.value().equals(Mapper.IGNORED_FIELDNAME)) {
                return me.value();
            }
        }

        return field.getName();
    }

    private static class FieldAccessor<T> implements PropertyAccessor<T> {
        private final Field field;

        private FieldAccessor(final Field field) {
            this.field = field;
        }

        @Override
        public T get(final Object instance) {
            try {
                return (T) field.get(instance);
            } catch (IllegalAccessException e) {
                throw new MappingException(e.getMessage(), e);
            }
        }

        @Override
        public void set(final Object instance, final Object value) {
            try {
                field.set(instance, value);
            } catch (IllegalAccessException e) {
                throw new MappingException(e.getMessage(), e);
            }
        }
    }

    private static class ArrayFieldAccessor<T> extends FieldAccessor<T> {

        private Class<?> componentType;

        private ArrayFieldAccessor(final Field field) {
            super(field);
            componentType = field.getType().getComponentType();
        }

        @Override
        public void set(final Object instance, final Object value) {
            Object newValue = value;
            if (value.getClass().getComponentType() != componentType) {
                Object[] array = (Object[]) value;
                final Object[] newArray = (Object[]) Array.newInstance(componentType, array.length);
                System.arraycopy(array, 0, newArray, 0, array.length);
                newValue = newArray;
            }
            super.set(instance, newValue);
        }
    }

    private static class MorphiaPropertySerialization implements PropertySerialization {
        private final List<Annotation> annotations;
        private MapperOptions options;
        private int modifiers;

        MorphiaPropertySerialization(final MapperOptions options, final FieldModelBuilder<?> field) {
            this.options = options;
            annotations = field.getAnnotations();
            modifiers = field.getField().getModifiers();
        }

        @Override
        public boolean shouldSerialize(final Object value) {
            if (!options.isStoreNulls() && value == null) {
                return false;
            }
            if (options.isIgnoreFinals() && Modifier.isFinal(modifiers)) {
                return false;
            }
            if (!options.isStoreEmpties()) {
                if (value instanceof Map && ((Map)value).isEmpty()
                    || value instanceof Collection && ((Collection)value).isEmpty()) {
                    return false;
                }
            }
            if (hasAnnotation(NotSaved.class)) {
                return false;
            }

            return true;
        }

        private boolean hasAnnotation(final Class<? extends Annotation> annotationClass) {
            return annotations.stream().anyMatch(a -> a.annotationType().equals(annotationClass));
        }
    }
}
