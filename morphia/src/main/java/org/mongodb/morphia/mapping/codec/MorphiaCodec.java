package org.mongodb.morphia.mapping.codec;

import org.bson.BsonDocumentReader;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.DiscriminatorLookup;
import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PojoCodec;
import org.bson.codecs.pojo.PojoCodecImpl;
import org.bson.codecs.pojo.PropertyCodecProvider;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.PostLoad;
import org.mongodb.morphia.annotations.PostPersist;
import org.mongodb.morphia.annotations.PreLoad;
import org.mongodb.morphia.annotations.PrePersist;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unchecked")
public class MorphiaCodec<T> extends PojoCodecImpl<T> implements CollectibleCodec<T> {
    private final Mapper mapper;
    private final MappedClass mappedClass;

    MorphiaCodec(final Mapper mapper, final MappedClass mappedClass, final ClassModel<T> classModel, final CodecRegistry registry,
                 final List<PropertyCodecProvider> propertyCodecProviders, final DiscriminatorLookup discriminatorLookup) {
        super(classModel, registry, propertyCodecProviders, discriminatorLookup);
        this.mapper = mapper;
        this.mappedClass = mappedClass;
    }

    private MorphiaCodec(final Mapper mapper, final MappedClass mappedClass, final ClassModel<T> classModel, final CodecRegistry registry,
                         final PropertyCodecRegistry propertyCodecRegistry, final DiscriminatorLookup discriminatorLookup,
                         final boolean specialized) {
        super(classModel, registry, propertyCodecRegistry, discriminatorLookup, new ConcurrentHashMap<>(), specialized);
        this.mapper = mapper;
        this.mappedClass = mappedClass;
    }

    @Override
    public T generateIdIfAbsentFromDocument(final T document) {
        if (!documentHasId(document)) {
            final MappedField mappedIdField = mappedClass.getMappedIdField();
            mappedIdField.setFieldValue(document, Conversions.convert(ObjectId.class, mappedIdField.getType(), new ObjectId()));
        }
        return document;
    }

    @Override
    public boolean documentHasId(final T document) {
        return mappedClass.getMappedIdField().getFieldValue(document) != null;
    }

    @Override
    public BsonValue getDocumentId(final T document) {
        final Object id = mappedClass.getMappedIdField().getFieldValue(document);
        final DocumentWriter writer = new DocumentWriter();
        final Codec codec = getRegistry().get(id.getClass());
        codec.encode(writer, id, EncoderContext.builder().build());
        return writer.getRoot();
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {

        if (mappedClass.hasLifecycle(PostPersist.class)
            || mappedClass.hasLifecycle(PrePersist.class)
            || mapper.hasInterceptors()) {
            final DocumentWriter documentWriter = new DocumentWriter();
            super.encode(documentWriter, value, encoderContext);
            Document document = documentWriter.getRoot();

            mappedClass.callLifecycleMethods(PrePersist.class, value, document, mapper);

            getRegistry().get(Document.class).encode(writer, document, encoderContext);

            mappedClass.callLifecycleMethods(PostPersist.class, value, document, mapper);

        } else {
            super.encode(writer, value, encoderContext);
        }
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        T entity;
        if (mappedClass.hasLifecycle(PreLoad.class) || mappedClass.hasLifecycle(PostLoad.class) || mapper.hasInterceptors()) {
            final InstanceCreator<T> instanceCreator = getClassModel().getInstanceCreator();
            entity = instanceCreator.getInstance();

            Document document = getRegistry().get(Document.class).decode(reader, decoderContext);
            mappedClass.callLifecycleMethods(PreLoad.class, entity, document, mapper);

            decodeProperties(new BsonDocumentReader(document.toBsonDocument(Document.class, mapper.getCodecRegistry())), decoderContext,
                instanceCreator);

            mappedClass.callLifecycleMethods(PostLoad.class, entity, document, mapper);
        } else {
            entity = super.decode(reader, decoderContext);
        }


        return entity;
    }

    public MappedClass getMappedClass() {
        return mappedClass;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <S> PojoCodec<S> getSpecializedCodec(final ClassModel<S> specialized) {
        return new SpecializedMorphiaCodec(this, specialized);
    }

    private static class SpecializedMorphiaCodec<T> extends PojoCodec<T> {

        private final MorphiaCodec morphiaCodec;
        private final ClassModel<T> classModel;
        private MorphiaCodec<T> specialized;

        SpecializedMorphiaCodec(final MorphiaCodec morphiaCodec, final ClassModel<T> classModel) {
            this.morphiaCodec = morphiaCodec;
            this.classModel = classModel;
        }

        @Override
        public ClassModel<T> getClassModel() {
            return classModel;
        }

        @Override
        public T decode(final BsonReader reader, final DecoderContext decoderContext) {
            return getSpecialized().decode(reader, decoderContext);
        }

        private MorphiaCodec<T> getSpecialized() {
            if (specialized == null) {
                specialized = new MorphiaCodec<>(morphiaCodec.mapper, new MappedClass(classModel, morphiaCodec.mapper),
                    classModel, morphiaCodec.getRegistry(), morphiaCodec.getPropertyCodecRegistry(), morphiaCodec.getDiscriminatorLookup(),
                    true);
            }
            return specialized;
        }

        @Override
        public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
            getSpecialized().encode(writer, value, encoderContext);
        }

        @Override
        public Class<T> getEncoderClass() {
            return classModel.getType();
        }
    }
}
