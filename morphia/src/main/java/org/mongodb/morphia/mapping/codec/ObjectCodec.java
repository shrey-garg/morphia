package org.mongodb.morphia.mapping.codec;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.BsonTypeCodecMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.mongodb.morphia.mapping.Mapper;

import static org.bson.codecs.BsonValueCodecProvider.getBsonTypeClassMap;

public class ObjectCodec implements Codec<Object> {

    private BsonTypeCodecMap bsonTypeCodecMap;
    private Mapper mapper;

    public ObjectCodec(final Mapper mapper) {
        this.mapper = mapper;
    }

    public BsonTypeCodecMap getBsonTypeCodecMap() {
        if (bsonTypeCodecMap == null) {
            this.bsonTypeCodecMap = new BsonTypeCodecMap(getBsonTypeClassMap(), mapper.getCodecRegistry());
        }
        return bsonTypeCodecMap;
    }

    @Override
    public Object decode(final BsonReader reader, final DecoderContext decoderContext) {
        final BsonType currentBsonType = reader.getCurrentBsonType();
        final Codec<?> codec = getBsonTypeCodecMap().get(currentBsonType);
        return codec.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        final Codec codec = mapper.getCodecRegistry().get(value.getClass());
        codec.encode(writer, value, encoderContext);
    }

    @Override
    public Class<Object> getEncoderClass() {
        return Object.class;
    }
}
