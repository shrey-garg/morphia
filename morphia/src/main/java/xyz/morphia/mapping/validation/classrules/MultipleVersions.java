package xyz.morphia.mapping.validation.classrules;


import xyz.morphia.annotations.Version;
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
 * A constraint to check for multiple version fields defined
 */
public class MultipleVersions implements ClassConstraint {

    @Override
    public void check(final Mapper mapper, final MappedClass mc, final Set<ConstraintViolation> ve) {
        final List<MappedField> versionFields = mc.getFieldsAnnotatedWith(Version.class);
        if (versionFields.size() > 1) {
            ve.add(new ConstraintViolation(Level.FATAL, mc, getClass(),
                format("Multiple @%s annotations are not allowed. (%s", Version.class, versionFields)));
        }
    }
}
