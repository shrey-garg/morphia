package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.mongodb.morphia.mapping.Mapper;

@SuppressWarnings("unchecked")
public class MorphiaTypesCodecProvider extends ValueCodecProvider {
    private final Codec<?> arrayCodec;
    private final Mapper mapper;

    public MorphiaTypesCodecProvider(final Mapper mapper) {
        addCodec(new KeyCodec(mapper));
        addCodec(new ClassCodec());
//        addCodec(new IntCodec());
        addCodec(new BooleanArrayCodec(mapper));
        addCodec(new ShortArrayCodec(mapper));
        addCodec(new IntArrayCodec(mapper));
        addCodec(new LongArrayCodec(mapper));
        addCodec(new FloatArrayCodec(mapper));
        addCodec(new DoubleArrayCodec(mapper));
        addCodec(new StringArrayCodec(mapper));
        arrayCodec = new ArrayCodec(mapper);
//        addCodec(arrayCodec);
        this.mapper = mapper;
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        final Codec<T> codec = super.get(clazz, registry);
        if(codec != null) {
            return codec;
        } else if( clazz.isArray()) {
            return (Codec<T>) arrayCodec;
        } else {
            return null;
        }
    }

}
