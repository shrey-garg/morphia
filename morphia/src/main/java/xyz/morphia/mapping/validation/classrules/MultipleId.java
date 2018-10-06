package xyz.morphia.mapping.validation.classrules;


import xyz.morphia.annotations.Id;
import xyz.morphia.mapping.MappedClass;
import xyz.morphia.mapping.MappedField;
import xyz.morphia.mapping.Mapper;
import xyz.morphia.mapping.validation.ClassConstraint;
import xyz.morphia.mapping.validation.ConstraintViolation;
import xyz.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.List;
import java.util.Set;

import static java.lang.String.format;

/**
 * A constraint to check for multiple ID fields defined
 */
public class MultipleId implements ClassConstraint {

    @Override
    public void check(final Mapper mapper, final MappedClass mc, final Set<ConstraintViolation> ve) {

        final List<MappedField> idFields = mc.getFieldsAnnotatedWith(Id.class);

        if (idFields.size() > 1) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, getClass(),
                format("More than one @%s Field found (%s).", Id.class.getSimpleName(), idFields)));
        }
    }

}
