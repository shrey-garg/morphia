package org.mongodb.morphia.mapping;

import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.PropertyAccessor;
import org.bson.codecs.pojo.PropertyMetadata;
import org.bson.codecs.pojo.PropertyModelBuilder;
import org.mongodb.morphia.annotations.Id;

import java.lang.reflect.Field;

import static org.bson.codecs.pojo.PojoBuilderHelper.createPropertyModelBuilder;

@SuppressWarnings("unchecked")
class MorphiaConvention implements Convention {
    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        classModelBuilder.getFieldModelBuilders().forEach(builder -> {
            PropertyModelBuilder<?> property = classModelBuilder.getProperty(builder.getName());

            if (property == null) {
                final PropertyMetadata<?> propertyMetadata = new PropertyMetadata<>(builder.getName(),
                    classModelBuilder.getType().getName(), builder.getTypeData())
                                                                 .field(builder.getField());
                property = createPropertyModelBuilder(propertyMetadata);
                classModelBuilder.addProperty(property);
            }

            if (builder.hasAnnotation(Id.class)) {
                builder
                    .readName(ClassModelBuilder.ID_PROPERTY_NAME)
                    .writeName(ClassModelBuilder.ID_PROPERTY_NAME);
                property
                    .readName(ClassModelBuilder.ID_PROPERTY_NAME)
                    .writeName(ClassModelBuilder.ID_PROPERTY_NAME);
            } else {
                property.readName(builder.getFieldName())
                        .writeName(builder.getFieldName());
            }
            property.propertyAccessor(new FieldAccessor(builder.getField()));

        });
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
