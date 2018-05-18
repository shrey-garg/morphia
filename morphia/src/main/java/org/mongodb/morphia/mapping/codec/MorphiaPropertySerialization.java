package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.pojo.FieldModelBuilder;
import org.bson.codecs.pojo.PropertySerialization;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.mapping.MapperOptions;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MorphiaPropertySerialization implements PropertySerialization {
    private final List<Annotation> annotations;
    private MapperOptions options;
    private int modifiers;

    public MorphiaPropertySerialization(final MapperOptions options, final FieldModelBuilder<?> field) {
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
