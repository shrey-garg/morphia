package org.mongodb.morphia.mapping.validation.classrules;


import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.validation.ClassConstraint;
import org.mongodb.morphia.mapping.validation.ConstraintViolation;
import org.mongodb.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * Checks that the entity is neither a {@code Map} nor and {@code Iterable}
 */
public class MustHaveNoArgConstructor implements ClassConstraint {

    @Override
    public void check(final Mapper mapper, final MappedClass mc, final Set<ConstraintViolation> ve) {

        try {
            mc.getClazz().getDeclaredConstructor();
        } catch (ReflectiveOperationException e) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, getClass(), "Mapped classes must have a 0 argument constructor"));
        }
    }
}
