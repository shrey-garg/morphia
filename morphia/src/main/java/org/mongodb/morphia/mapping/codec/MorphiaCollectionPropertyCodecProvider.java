package org.mongodb.morphia.mapping.codec;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.CollectionCodec;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.TypeData;
import org.bson.codecs.pojo.TypeWithTypeParameters;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressWarnings("unchecked")
public class MorphiaCollectionPropertyCodecProvider extends MorphiaPropertyCodecProvider {
    @Override
    public <T> Codec<T> get(final TypeWithTypeParameters<T> type, final PropertyCodecRegistry registry) {
        if (Collection.class.isAssignableFrom(type.getType())) {
            final List<? extends TypeWithTypeParameters<?>> typeParameters = type.getTypeParameters();
            TypeWithTypeParameters<?> valueType = getType(typeParameters, 0);

            try {
                return new ListCodec(type.getType(), registry.get(valueType));
            } catch (CodecConfigurationException e) {
                if (valueType.getType().equals(Object.class)) {
                    try {
                        return (Codec<T>) registry.get(TypeData.builder(Collection.class).build());
                    } catch (CodecConfigurationException e1) {
                        // Ignore and return original exception
                    }
                }
                throw e;
            }
        }

        return null;
    }

    private class ListCodec<T> extends CollectionCodec<T> {
        ListCodec(final Class<Collection<T>> encoderClass, final Codec<T> codec) {
            super(encoderClass, codec);
        }

        @Override
        public Collection<T> decode(final BsonReader reader, final DecoderContext decoderContext) {
            if(reader.getCurrentBsonType().equals(BsonType.ARRAY)) {
                return super.decode(reader, decoderContext);
            }
            final Collection<T> collection = getInstance();
            collection.add(getCodec().decode(reader, decoderContext));
            return collection;
        }

        private Collection<T> getInstance() {
            if (getEncoderClass().equals(Collection.class) || getEncoderClass().equals(List.class)) {
                return new ArrayList<>();
            } else if (getEncoderClass().equals(Set.class)) {
                return new HashSet<>();
            }
            try {
                final Constructor<Collection<T>> constructor = getEncoderClass().getDeclaredConstructor();
                constructor.setAccessible(true);
                return constructor.newInstance();
            } catch (final Exception e) {
                throw new CodecConfigurationException(e.getMessage(), e);
            }
        }

    }
}
