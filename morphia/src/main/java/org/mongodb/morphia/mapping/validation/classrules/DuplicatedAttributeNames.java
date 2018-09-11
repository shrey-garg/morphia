package org.mongodb.morphia.mapping.validation.classrules;


import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.validation.ClassConstraint;
import org.mongodb.morphia.mapping.validation.ConstraintViolation;
import org.mongodb.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.HashSet;
import java.util.Set;

import static java.lang.String.format;

/**
 * A constraint to check for duplicated attribute names
 */
public class DuplicatedAttributeNames implements ClassConstraint {

    @Override
    public void check(final Mapper mapper, final MappedClass mc, final Set<ConstraintViolation> ve) {
        final Set<String> foundNames = new HashSet<>();
        for (final MappedField mappedField : mc.getPersistenceFields()) {
            if (!foundNames.add(mappedField.getNameToStore())) {
                ve.add(new ConstraintViolation(Level.FATAL, mc, mappedField, getClass(),
                    format("Mapping to MongoDB field name '%s' is duplicated; you cannot map different java fields to the same "
                           + "MongoDB field.", mappedField.getNameToStore())));
            }
        }
    }
}
