package xyz.morphia.mapping.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.HashMap;
import java.util.Map;

/**
 * A provider of Codecs for primitive types.
 */
@SuppressWarnings("unchecked")
public class PrimitiveCodecProvider implements CodecRegistry {
    private Map<Class, Codec> primitiveCodecs = new HashMap<>();

    /**
     * Creates the provider
     *
     * @param codecRegistry the CodecRegistry to use
     */
    public PrimitiveCodecProvider(final CodecRegistry codecRegistry) {
        primitiveCodecs.put(byte.class, codecRegistry.get(Byte.class));
        primitiveCodecs.put(short.class, codecRegistry.get(Short.class));
        primitiveCodecs.put(int.class, codecRegistry.get(Integer.class));
        primitiveCodecs.put(long.class, codecRegistry.get(Long.class));
        primitiveCodecs.put(float.class, codecRegistry.get(Float.class));
        primitiveCodecs.put(double.class, codecRegistry.get(Double.class));
        primitiveCodecs.put(boolean.class, codecRegistry.get(Boolean.class));
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz) {
        return primitiveCodecs.get(clazz);
    }
}
