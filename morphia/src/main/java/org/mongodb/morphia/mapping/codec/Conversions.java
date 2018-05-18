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

import static java.lang.String.format;

public final class Conversions {

    private static final Logger LOG = MorphiaLoggerFactory.get(Conversions.class);
    private static Map<Class<?>, Map<Class<?>, Function<?, ?>>> conversions  = new HashMap<>();

    static {
        register(String.class, ObjectId.class, ObjectId::new);
        register(String.class, Boolean.class, Boolean::parseBoolean);
        register(String.class, Byte.class, Byte::parseByte);
        register(String.class, Double.class, Double::parseDouble);
        register(String.class, Integer.class, Integer::parseInt);
        register(String.class, Long.class, Long::parseLong);
        register(String.class, Float.class, Float::parseFloat);
        register(String.class, Short.class, Short::parseShort);

        register(URI.class, String.class, u -> {
            try {
                return u.toURL().toExternalForm().replace(".", "%46");
            } catch (MalformedURLException e) {
                throw new MappingException("Could not convert URI: " + u);
            }
        });
        register(String.class, URI.class, str -> URI.create(str.replace("%46", ".")));

        register(Binary.class, byte[].class, Binary::getData);
    }

    private static <F, T> void register(final Class<F> fromType, final Class<T> toType, final Function<F, T> function) {
        conversions.computeIfAbsent(fromType, (Class<?> c) -> new HashMap<>())
                   .put(toType, function);
    }

    @SuppressWarnings("unchecked")
    public static Object convert(Class<?> fromType, Class<?> toType, Object value) {
        if(fromType.equals(toType)) {
            return value;
        }

        final Map<Class<?>, Function<?, ?>> functions = conversions.computeIfAbsent(fromType, (Class f) -> new HashMap<>());
        final Function function = functions.get(toType);
        if(function == null) {
            if(toType.equals(String.class)) {
                return value.toString();
            }
            if(toType.isEnum() && fromType.equals(String.class)) {
                return Enum.valueOf((Class<? extends Enum>)toType, (String)value);
            }
            return value;
        }
        return function.apply(value);
    }
}
