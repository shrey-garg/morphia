package org.mongodb.morphia.query.validation;

import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.query.FilterOperator;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.mongodb.morphia.query.FilterOperator.MOD;


/**
 * Validates queries using the {@code FilterOperator.MOD}.
 *
 * @mongodb.driver.manual reference/operator/query/mod/ $mod
 */
public final class ModOperationValidator extends OperationValidator {
    private static final ModOperationValidator INSTANCE = new ModOperationValidator();

    private ModOperationValidator() {
    }

    /**
     * Get the instance.
     *
     * @return the Singleton instance of this validator
     */
    public static ModOperationValidator getInstance() {
        return INSTANCE;
    }

    @Override
    protected FilterOperator getOperator() {
        return MOD;
    }

    @Override
    protected void validate(final MappedField mappedField, final Object value, final List<ValidationFailure> validationFailures) {
        if (value == null) {
            validationFailures.add(new ValidationFailure("For a $mod operation, value cannot be null."));
        } else if (value.getClass().isArray()) {
            if (Array.getLength(value) != 2) {
                validationFailures.add(new ValidationFailure(format("For a $mod operation, value '%s' should be an array with two integer"
                                                                    + " elements.  Instead it had %s", value, Array.getLength(value))));
            }
            if (!(isIntegerType(value.getClass().getComponentType()))) {
                validationFailures.add(new ValidationFailure(format("Array value needs to contain integers for $mod, but contained: %s",
                                                                    value.getClass().getComponentType())));
            }
        } else {
            validationFailures.add(new ValidationFailure(format("For a $mod operation, value '%s' should be an integer array.  "
                                                                + "Instead it was a: %s", value, value.getClass())));
        }
    }

    /**
     * Checks if the class is an integer type, i.e., is numeric but not a floating point type.
     *
     * @param type the class we want to check
     * @return true if the type is an integral type
     */
    private static boolean isIntegerType(final Class type) {
        return Arrays.<Class>asList(Integer.class, int.class, Long.class, long.class, Short.class, short.class, Byte.class,
            byte.class).contains(type);
    }
}
