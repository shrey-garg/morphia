package org.mongodb.morphia.mapping;

import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.FieldModelBuilder;
import org.bson.codecs.pojo.PropertyAccessor;
import org.bson.codecs.pojo.PropertyMetadata;
import org.bson.codecs.pojo.PropertyModelBuilder;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.annotations.Serialized;
import org.mongodb.morphia.annotations.Transient;
import org.mongodb.morphia.annotations.Version;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.bson.codecs.pojo.PojoBuilderHelper.createPropertyModelBuilder;

@SuppressWarnings("unchecked")
class MorphiaConvention implements Convention {
    private MapperOptions opts;

    public MorphiaConvention(final MapperOptions opts) {
        this.opts = opts;
    }

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        classModelBuilder
            .getFieldModelBuilders()
            .forEach(builder -> {
                final Field field = builder.getField();

                PropertyModelBuilder<?> property = classModelBuilder.getProperty(builder.getName());

                if (Modifier.isStatic(field.getModifiers()) || isTransient(builder)) {
                    classModelBuilder.removeField(field.getName());
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

                    final String mappedName = getMappedFieldName(builder);
/*
                if (builder.hasAnnotation(Id.class)) {
                    builder
                        .readName(ClassModelBuilder.ID_PROPERTY_NAME)
                        .writeName(ClassModelBuilder.ID_PROPERTY_NAME);
                    property
                        .readName(ClassModelBuilder.ID_PROPERTY_NAME)
                        .writeName(ClassModelBuilder.ID_PROPERTY_NAME);
                }
*/
                    property.readName(mappedName)
                            .writeName(mappedName)
                            .propertyAccessor(new FieldAccessor(field));

                }
            });
    }

    private static boolean isTransient(final FieldModelBuilder<?> field) {
        return field.hasAnnotation(Transient.class)
               && field.hasAnnotation(java.beans.Transient.class)
               && Modifier.isTransient(field.getTypeData().getType().getModifiers());
    }

    private static String   getMappedFieldName(FieldModelBuilder<?> field) {
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
}
