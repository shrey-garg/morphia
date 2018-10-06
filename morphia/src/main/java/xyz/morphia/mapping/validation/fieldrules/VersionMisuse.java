package xyz.morphia.mapping.validation.fieldrules;


import xyz.morphia.ObjectFactory;
import xyz.morphia.annotations.Version;
import xyz.morphia.mapping.MappedClass;
import xyz.morphia.mapping.MappedField;
import xyz.morphia.mapping.Mapper;
import xyz.morphia.mapping.validation.ConstraintViolation;
import xyz.morphia.mapping.validation.ConstraintViolation.Level;

import java.util.Set;

import static java.lang.String.format;

/**
 * A constraint checking for invalid {@code @Version} set up
 */
public class VersionMisuse extends FieldConstraint {

    private ObjectFactory creator;

    /**
     * Creates a version validator.
     *
     * @param creator the ObjectFactory to use
     */
    public VersionMisuse(final ObjectFactory creator) {
        this.creator = creator;
    }

    @Override
    protected void check(final Mapper mapper, final MappedClass mc, final MappedField mf, final Set<ConstraintViolation> ve) {
        if (mf.hasAnnotation(Version.class) && !mc.isAbstract()) {
            final Class<?> type = mf.getType();
            if (Long.class.equals(type) || long.class.equals(type)) {

                final Object testInstance = creator.createInstance(mc.getClazz());

                final Object value = mf.getFieldValue(testInstance);
                if (value != null && (!value.equals(0L))) {
                    ve.add(new ConstraintViolation(Level.FATAL, mc, mf, getClass(),
                        format("When using @%s on a field, it must be initialized to null or 0.",
                            Version.class.getSimpleName())));
                }
            } else {
                ve.add(new ConstraintViolation(Level.FATAL, mc, mf, getClass(),
                    format("@%s can only be used on a Long/long field.", Version.class.getSimpleName())));
            }
        }
    }
}

