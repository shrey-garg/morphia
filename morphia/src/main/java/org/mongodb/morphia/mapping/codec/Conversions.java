package org.mongodb.morphia.mapping.codec;

import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;
import org.mongodb.morphia.mapping.MappingException;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.Boolean.FALSE;

/**
 * Defines simple type conversions
 */
public final class Conversions {
    private static final Logger LOG = MorphiaLoggerFactory.get(Conversions.class);
    private static Map<Class<?>, Map<Class<?>, Function<?, ?>>> conversions = new HashMap<>();

    static {
        register(String.class, ObjectId.class, ObjectId::new);
        register(String.class, Boolean.class, Boolean::parseBoolean);
        register(String.class, Byte.class, Byte::parseByte);
        register(String.class, Double.class, Double::parseDouble);
        register(String.class, Integer.class, Integer::parseInt);
        register(String.class, Long.class, Long::parseLong);
        register(String.class, Float.class, Float::parseFloat);
        register(String.class, Short.class, Short::parseShort);

        register(Binary.class, byte[].class, Binary::getData);

        register(Double.class, Long.class, Double::longValue, "Converting a double value to a long.  Possible loss of precision.");
        register(Long.class, Double.class, Long::doubleValue);

        register(Float.class, Long.class, Float::longValue, "Converting a float value to a long.  Possible loss of precision.");
        register(Long.class, Float.class, Long::floatValue);

        register(String.class, URI.class, str -> URI.create(str.replace("%46", ".")));
        register(URI.class, String.class, u -> {
            try {
                return u.toURL().toExternalForm().replace(".", "%46");
            } catch (MalformedURLException e) {
                throw new MappingException("Could not convert URI: " + u);
            }
        });
    }

    private Conversions() {
    }

    private static <F, T> void register(final Class<F> fromType, final Class<T> toType, final Function<F, T> function) {
        register(fromType, toType, function, null);
    }

    private static <F, T> void register(final Class<F> fromType, final Class<T> toType, final Function<F, T> function,
                                        final String warning) {
        final Function<F, T> conversion =
            warning == null
            ? function
            : f -> {
                if (LOG.isWarningEnabled()) {
                    LOG.warning(warning);
                }
                return function.apply(f);
            };
        conversions.computeIfAbsent(fromType, (Class<?> c) -> new HashMap<>())
                   .put(toType, conversion);
    }

    /**
     * Converts a value
     * @param value the value to convert
     * @param toType the target type
     * @return the converted value
     */
    @SuppressWarnings("unchecked")
    public static Object convert(final Object value, final Class<?> toType) {
        if (value == null) {
            return convertNull(toType);
        }

        final Class<?> fromType = value.getClass();
        if (fromType.equals(toType)) {
            return value;
        }

        final Map<Class<?>, Function<?, ?>> functions = conversions.computeIfAbsent(fromType, (Class f) -> new HashMap<>());
        final Function function = functions.get(toType);
        if (function == null) {
            if (toType.equals(String.class)) {
                return value.toString();
            }
            if (toType.isEnum() && fromType.equals(String.class)) {
                return Enum.valueOf((Class<? extends Enum>) toType, (String) value);
            }
            return value;
        }
        return function.apply(value);
    }

    private static Object convertNull(final Class<?> toType) {
        if (isNumber(toType)) {
            return 0;
        } else if (isBoolean(toType)) {
            return FALSE;
        }
        return null;
    }

    private static boolean isNumber(final Class<?> type) {
        return type.isPrimitive() && !type.equals(boolean.class);
    }

    private static boolean isBoolean(final Class<?> type) {
        return type.equals(boolean.class);
    }
}
