package org.mongodb.morphia.mapping.validation.fieldrules;


import org.bson.codecs.pojo.TypeData;
import org.mongodb.morphia.annotations.Serialized;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.validation.ConstraintViolation;
import org.mongodb.morphia.mapping.validation.ConstraintViolation.Level;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class MapNotSerializable extends FieldConstraint {

    @Override
    protected void check(final Mapper mapper, final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.isMap() && mf.hasAnnotation(Serialized.class)) {
            final List<TypeData<?>> typeData = mf.getTypeData().getTypeParameters();
            final Class<?> keyClass = typeData.get(0).getType();
            final Class<?> valueClass = typeData.get(1).getType();
            if (keyClass != null) {
                if (!Serializable.class.isAssignableFrom(keyClass)) {
                    ve.add(new ConstraintViolation(Level.FATAL, mc, mf, getClass(),
                        "Key class (" + keyClass.getName() + ") is not Serializable"));
                }
            }
            if (valueClass != null) {
                if (!Serializable.class.isAssignableFrom(valueClass)) {
                    ve.add(new ConstraintViolation(Level.FATAL, mc, mf, getClass(),
                        "Value class (" + valueClass.getName() + ") is not Serializable"));
                }
            }
        }
    }
}
