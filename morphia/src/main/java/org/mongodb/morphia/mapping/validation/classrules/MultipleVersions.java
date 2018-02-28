package org.mongodb.morphia.mapping.validation.classrules;


import org.mongodb.morphia.annotations.Version;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.validation.ClassConstraint;
import org.mongodb.morphia.mapping.validation.ConstraintViolation;
import org.mongodb.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.List;
import java.util.Set;

import static java.lang.String.format;

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
