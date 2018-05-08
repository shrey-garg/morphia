package org.mongodb.morphia.mapping.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.mongodb.morphia.mapping.Mapper;

public class ObjectCodec implements Codec<Object> {

    private Mapper mapper;

    public ObjectCodec(final Mapper mapper) {

        this.mapper = mapper;
    }

    @Override
    public Object decode(final BsonReader reader, final DecoderContext decoderContext) {
        return null;
    }

    @Override
    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        final Codec<?> codec = mapper.getCodecRegistry().get(value.getClass());
//        codec.encode(writer);

    }

    @Override
    public Class<Object> getEncoderClass() {
        return Object.class;
    }
}
