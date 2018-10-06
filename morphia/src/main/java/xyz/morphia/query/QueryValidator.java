package xyz.morphia.query;

import xyz.morphia.annotations.Serialized;
import xyz.morphia.logging.Logger;
import xyz.morphia.logging.MorphiaLoggerFactory;
import xyz.morphia.mapping.MappedClass;
import xyz.morphia.mapping.MappedField;
import xyz.morphia.mapping.Mapper;
import xyz.morphia.query.validation.AllOperationValidator;
import xyz.morphia.query.validation.DefaultTypeValidator;
import xyz.morphia.query.validation.DoubleTypeValidator;
import xyz.morphia.query.validation.EntityAnnotatedValueValidator;
import xyz.morphia.query.validation.EntityTypeAndIdValueValidator;
import xyz.morphia.query.validation.ExistsOperationValidator;
import xyz.morphia.query.validation.GeoWithinOperationValidator;
import xyz.morphia.query.validation.InOperationValidator;
import xyz.morphia.query.validation.IntegerTypeValidator;
import xyz.morphia.query.validation.KeyValueTypeValidator;
import xyz.morphia.query.validation.ListValueValidator;
import xyz.morphia.query.validation.LongTypeValidator;
import xyz.morphia.query.validation.ModOperationValidator;
import xyz.morphia.query.validation.NotInOperationValidator;
import xyz.morphia.query.validation.PatternValueValidator;
import xyz.morphia.query.validation.SizeOperationValidator;
import xyz.morphia.query.validation.ValidationFailure;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

final class QueryValidator {
    private static final Logger LOG = MorphiaLoggerFactory.get(QueryValidator.class);

    private QueryValidator() {
    }

    /**
     * Validate the path, and value type, returning the mapped field for the field at the path
     */
    static MappedField validateQuery(final Class clazz, final Mapper mapper, final StringBuilder origProp, final FilterOperator op,
                                     final Object val, final boolean validateNames, final boolean validateTypes) {
        MappedField mf = null;
        final String prop = origProp.toString();
        boolean hasTranslations = false;

        if (!origProp.substring(0, 1).equals("$")) {
            final String[] parts = prop.split("\\.");
            if (clazz == null) {
                return null;
            }

            MappedClass mc = mapper.getMappedClass(clazz);
            //CHECKSTYLE:OFF
            int i = 0;
            while (true) {
                //CHECKSTYLE:ON
                final String part = parts[i];
                boolean fieldIsArrayOperator = part.equals("$") || part.matches("[0-9]+");

                mf = mc != null ? mc.getMappedField(part) : null;

                //translate from java field name to stored field name
                if (mf == null && !fieldIsArrayOperator) {
                    mf = mc != null ? mc.getMappedFieldByJavaField(part) : null;
                    if (validateNames && mf == null) {
                        throw new ValidationException(format("The path '%s' is not a valid path for %s; if "
                                                             + "you wish to continue please disable validation.", origProp,
                            clazz));
                    }
                    hasTranslations = true;
                    if (mf != null) {
                        parts[i] = mf.getNameToStore();
                    }
                }

                i++;
                if (mf != null && mf.isMap()) {
                    //skip the map key validation, and move to the next part
                    i++;
                }

                if (i >= parts.length) {
                    break;
                }

                if (!fieldIsArrayOperator) {
                    //catch people trying to search/update into @Reference/@Serialized fields
                    if (validateNames && !canQueryPast(mf)) {
                        throw new ValidationException(format("Cannot use dot-notation past '%s' in '%s'; found while"
                                                             + " validating - %s", part, mc.getClazz().getName(), prop));
                    }

                    if (mf == null) {
                        break;
                    }
                    //get the next MappedClass for the next field validation
                    mc = mapper.getMappedClass(mf.getNormalizedType());
                }
            }

            //record new property string if there has been a translation to any part
            if (hasTranslations) {
                origProp.setLength(0); // clear existing content
                origProp.append(parts[0]);
                for (int index = 1; index < parts.length; index++) {
                    origProp.append('.');
                    origProp.append(parts[index]);
                }
            }

            if (validateTypes && mf != null) {
                List<ValidationFailure> failures = new ArrayList<>();
                boolean compatibleForType = isCompatibleForOperator(mc, mf, mf.getType(), op, val, failures);
                boolean compatibleForSubclass = isCompatibleForOperator(mc, mf, mf.getSpecializedType(), op, val, failures);

                if ((mf.isScalarValue() && !compatibleForType)
                    || mf.isMultipleValues() && !(compatibleForSubclass || compatibleForType)) {

                    if (LOG.isWarningEnabled()) {
                        LOG.warning(format("The type(s) for the query/update may be inconsistent; using an instance of type '%s' "
                                           + "for the field '%s.%s' which is declared as '%s'", val.getClass().getName(),
                            mf.getDeclaringClass().getClazz().getName(), mf.getJavaFieldName(), mf.getType().getName()));
                        LOG.warning("Validation warnings: \n" + failures);
                    }
                }
            }
        }

        return mf;
    }

    private static boolean canQueryPast(final MappedField mf) {
        return !(mf.isReference() || mf.hasAnnotation(Serialized.class));
    }

    /*package*/
    static boolean isCompatibleForOperator(final MappedClass mappedClass, final MappedField mappedField, final Class<?> type,
                                           final FilterOperator op, final Object value, final List<ValidationFailure> validationFailures) {
        // TODO: it's really OK to have null values?  I think this is to prevent null pointers further down,
        // but I want to move the null check into the operations that care whether they allow nulls or not.
        if (value == null || type == null) {
            return true;
        }

        boolean validationApplied = ExistsOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || SizeOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || InOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || NotInOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || ModOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || GeoWithinOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || AllOperationValidator.getInstance().apply(mappedField, op, value, validationFailures)
                                    || KeyValueTypeValidator.getInstance().apply(type, value, validationFailures)
                                    || IntegerTypeValidator.getInstance().apply(type, value, validationFailures)
                                    || LongTypeValidator.getInstance().apply(type, value, validationFailures)
                                    || DoubleTypeValidator.getInstance().apply(type, value, validationFailures)
                                    || PatternValueValidator.getInstance().apply(type, value, validationFailures)
                                    || EntityAnnotatedValueValidator.getInstance().apply(type, value, validationFailures)
                                    || ListValueValidator.getInstance().apply(type, value, validationFailures)
                                    || EntityTypeAndIdValueValidator.getInstance()
                                                                    .apply(mappedClass, mappedField, value, validationFailures)
                                    || DefaultTypeValidator.getInstance().apply(type, value, validationFailures);

        return validationApplied && validationFailures.size() == 0;
    }

}
