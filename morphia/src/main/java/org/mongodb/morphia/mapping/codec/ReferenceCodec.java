package org.mongodb.morphia.mapping.codec;

import com.mongodb.DBRef;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.pojo.PropertyModel;
import org.mongodb.morphia.annotations.Reference;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;

public class ReferenceCodec {

    private Mapper mapper;
    private final PropertyModel propertyModel;
    private final MappedField field;
    private BsonTypeClassMap bsonTypeClassMap = new BsonTypeClassMap();
    private final MappedField idField;
    private final MappedClass mappedClass;

    public ReferenceCodec(final Mapper mapper, final PropertyModel propertyModel, final MappedField field) {
        this.mapper = mapper;
        this.propertyModel = propertyModel;
        this.field = field;
        final MorphiaCodec codec = (MorphiaCodec) mapper.getCodecRegistry().get(field.getType());
        mappedClass = codec.getMappedClass();

        idField = mappedClass.getIdField();
    }

    public <S> S decode(final BsonReader reader, final DecoderContext decoderContext) {
        return (S) mapper.getCodecRegistry()
                         .get(bsonTypeClassMap.get(reader.getCurrentBsonType()))
                         .decode(reader, decoderContext);
    }

    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        if (value == null) {
            writer.writeNull(propertyModel.getReadName());
        } else {
            writer.writeName(propertyModel.getReadName());
            Object idValue = idField.getFieldValue(value);
            if(!field.getAnnotation(Reference.class).idOnly()) {
                idValue = new DBRef(mappedClass.getCollectionName(), idValue);
            }

            final Codec codec = mapper.getCodecRegistry().get(idValue.getClass());
            codec.encode(writer, idValue, encoderContext);
        }
    }
}
