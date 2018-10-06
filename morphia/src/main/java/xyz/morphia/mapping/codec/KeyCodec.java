package xyz.morphia.mapping.codec;

import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import xyz.morphia.Key;
import xyz.morphia.mapping.MappedClass;
import xyz.morphia.mapping.MappedField;
import xyz.morphia.mapping.Mapper;
import xyz.morphia.mapping.MappingException;

import java.util.Iterator;
import java.util.List;

/**
 * A {@link Codec} for {@link Key}
 */
@SuppressWarnings("unchecked")
public class KeyCodec implements Codec<Key> {

    private Mapper mapper;

    KeyCodec(final Mapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void encode(final BsonWriter writer, final Key value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        String collection = value.getCollection();
        if (collection == null) {
            collection = mapper.getCollectionName(value.getType());
        }
        writer.writeString("$ref", collection);
        writer.writeName("$id");
        Codec codec = mapper.getCodecRegistry().get(value.getId().getClass());
        codec.encode(writer, value.getId(), encoderContext);
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
        final List<MappedClass> classes = mapper.getClassesMappedToCollection(ref);
        reader.readName();
        final BsonReaderMark mark = reader.getMark();
        final Iterator<MappedClass> iterator = classes.iterator();
        Object idValue = null;
        MappedClass mappedClass = null;
        while (idValue == null && iterator.hasNext()) {
            mappedClass = iterator.next();
            try {
                final MappedField idField = mappedClass.getIdField();
                if (idField != null) {
                    final Class<?> idType = idField.getTypeData().getType();
                    idValue = mapper.getCodecRegistry().get(idType).decode(reader, decoderContext);
                }
            } catch (Exception e) {
                mark.reset();
            }
        }

        if (idValue == null) {
            throw new MappingException("Could not map the Key to a type.");
        }
        reader.readEndDocument();
        return new Key<>(mappedClass.getClazz(), ref, idValue);
    }
}
