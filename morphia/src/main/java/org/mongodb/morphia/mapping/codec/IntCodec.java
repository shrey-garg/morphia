package org.mongodb.morphia.mapping.codec;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * A {@link Codec} for Integer
 */
public class IntCodec implements Codec<Integer> {
    @Override
    public Integer decode(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return 0;
        } else {
            return reader.readInt32();
        }
    }

    @Override
    public void encode(final BsonWriter writer, final Integer value, final EncoderContext encoderContext) {
        writer.writeInt32(value);
    }

    @Override
    public Class getEncoderClass() {
        return int.class;
    }
}
