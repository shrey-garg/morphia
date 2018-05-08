package org.mongodb.morphia.mapping.codec;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.MappingException;

public class KeyCodec implements Codec<Key> {

    private Mapper mapper;

    public KeyCodec(final Mapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void encode(final BsonWriter writer, final Key value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        String collection = value.getCollection();
        if(collection == null) {
            collection = mapper.getCollectionName(value.getType());
        }
        writer.writeString("$ref", collection);
        writer.writeName("$id");
        Codec codec = mapper.getCodecRegistry().get(value.getId().getClass());
        codec.encode(writer, value.getId(), encoderContext);
        writer.writeString("type", value.getType().getName());
        writer.writeEndDocument();
    }

    @Override
    public Class<Key> getEncoderClass() {
        return Key.class;
    }

    @Override
    public Key decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartDocument();

        final String ref = reader.readString("$ref");
        final Class<?> classFromCollection = mapper.getClassFromCollection(ref);
        final MappedClass mappedClass = mapper.getMappedClass(classFromCollection);

        reader.readName();
        final Class<?> idType = mappedClass.getIdField().getTypeData().getType();
        final Object idValue = mapper.getCodecRegistry().get(idType).decode(reader, decoderContext);
        final String type = reader.readString("type");

        reader.readEndDocument();
        return new Key<>(classFromCollection, ref, idValue);
    }
}
