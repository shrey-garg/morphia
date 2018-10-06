package xyz.morphia.mapping.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.MapCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import xyz.morphia.mapping.Mapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Defines a {@code ValueCodecProvider} for common types used in entities.
 */
@SuppressWarnings("unchecked")
public class MorphiaTypesCodecProvider extends ValueCodecProvider {
    private final Codec<?> arrayCodec;

    /**
     * Creates a new provider
     *
     * @param mapper the mapper to use
     */
    public MorphiaTypesCodecProvider(final Mapper mapper) {
        addCodec(new KeyCodec(mapper));
        addCodec(new ClassCodec());
        addCodec(new BooleanArrayCodec(mapper));
        addCodec(new ShortArrayCodec(mapper));
        addCodec(new IntArrayCodec(mapper));
        addCodec(new LongArrayCodec(mapper));
        addCodec(new FloatArrayCodec(mapper));
        addCodec(new DoubleArrayCodec(mapper));
        addCodec(new StringArrayCodec(mapper));
        addCodec(new HashMapCodec());
        addCodec(new URICodec());
        addCodec(new ObjectCodec(mapper));
        arrayCodec = new ArrayCodec(mapper);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        final Codec<T> codec = super.get(clazz, registry);
        if (codec != null) {
            return codec;
        } else if (clazz.isArray() && !clazz.getComponentType().equals(byte.class)) {
            return (Codec<T>) arrayCodec;
        } else {
            return null;
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final MorphiaTypesCodecProvider that = (MorphiaTypesCodecProvider) o;

        return arrayCodec != null ? arrayCodec.equals(that.arrayCodec) : that.arrayCodec == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (arrayCodec != null ? arrayCodec.hashCode() : 0);
        return result;
    }

    private static class HashMapCodec extends MapCodec {
        @Override
        public Class<Map<String, Object>> getEncoderClass() {
            return (Class<Map<String, Object>>) ((Class) HashMap.class);
        }
    }
}
