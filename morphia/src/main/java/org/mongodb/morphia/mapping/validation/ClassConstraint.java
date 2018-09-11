package org.mongodb.morphia.mapping.validation;


import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;

import java.util.Set;

/**
 * Defines a constraint to be applied to a class.
 */
public interface ClassConstraint {
    /**
     * Check that a MappedClass meets the constraint
     *
     * @param mc     the MappedClass to check
     * @param ve     the set of violations
     * @param mapper the Mapper to use for validation
     */
    void check(Mapper mapper, MappedClass mc, Set<ConstraintViolation> ve);
}
