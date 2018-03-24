package org.mongodb.morphia.mapping.codec;

import org.bson.BsonArray;
import org.bson.BsonBinarySubType;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.mongodb.morphia.mapping.Mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

class ArrayCodec implements Codec<Object[]> {

    private Mapper mapper;

    public ArrayCodec(final Mapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Class<Object[]> getEncoderClass() {
        return Object[].class;
    }

    @Override
    public void encode(final BsonWriter writer, final Object[] value, final EncoderContext encoderContext) {
        writer.writeStartArray();
        for (final Object cur : value) {
//            writeValue(writer, encoderContext, cur);
        }
        writer.writeEndArray();
    }

    @Override
    public Object[] decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();

        List<Object> list = new ArrayList<Object>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext));
        }

        reader.readEndArray();

        return list.toArray();
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else if (bsonType == BsonType.BINARY && BsonBinarySubType.isUuid(reader.peekBinarySubType()) && reader.peekBinarySize() == 16) {
            return mapper.getCodecRegistry().get(UUID.class).decode(reader, decoderContext);
        }
        return null; //valueTransformer.transform(bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext));
    }

}
