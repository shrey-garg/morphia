package org.mongodb.morphia.mapping.validation.classrules;


import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.validation.ClassConstraint;
import org.mongodb.morphia.mapping.validation.ConstraintViolation;
import org.mongodb.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * A constraint to check that an ID field has been specified
 */
public class NoId implements ClassConstraint {

    @Override
    public void check(final Mapper mapper, final MappedClass mc, final Set<ConstraintViolation> ve) {
        if (!mc.isAbstract() && mc.getIdField() == null && mc.getEntityAnnotation() != null) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, getClass(), "No field is annotated with @Id; but it is required"));
        }
    }

}
