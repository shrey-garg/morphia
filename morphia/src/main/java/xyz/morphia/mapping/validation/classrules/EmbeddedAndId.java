package xyz.morphia.mapping.validation.classrules;


import xyz.morphia.annotations.Embedded;
import xyz.morphia.mapping.MappedClass;
import xyz.morphia.mapping.Mapper;
import xyz.morphia.mapping.validation.ClassConstraint;
import xyz.morphia.mapping.validation.ConstraintViolation;
import xyz.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

/**
 * A constraint to check that {@code @Embedded} types don't specify an {@code @Id} field
 */
public class EmbeddedAndId implements ClassConstraint {

    @Override
    public void check(final Mapper mapper, final MappedClass mc, final Set<ConstraintViolation> ve) {
        if (mc.getEmbeddedAnnotation() != null && mc.getIdField() != null) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, getClass(),
                String.format("@%s classes cannot specify a @Id field", Embedded.class.getSimpleName())));
        }
    }

}
