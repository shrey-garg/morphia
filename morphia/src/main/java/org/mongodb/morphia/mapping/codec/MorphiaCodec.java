package org.mongodb.morphia.mapping.codec;

import org.bson.BsonDocumentReader;
import org.bson.BsonReader;
import org.bson.BsonReaderMark;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.DiscriminatorLookup;
import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PojoCodec;
import org.bson.codecs.pojo.PojoCodecImpl;
import org.bson.codecs.pojo.PropertyCodecProvider;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.PropertyModel;
import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.annotations.PostLoad;
import org.mongodb.morphia.annotations.PostPersist;
import org.mongodb.morphia.annotations.PreLoad;
import org.mongodb.morphia.annotations.PrePersist;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.MorphiaInstanceCreator;
import org.mongodb.morphia.mapping.PropertyHandler;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.mongodb.morphia.mapping.codec.Conversions.convert;

@SuppressWarnings("unchecked")
public class MorphiaCodec<T> extends PojoCodecImpl<T> implements CollectibleCodec<T> {
    private final Datastore datastore;
    private final Mapper mapper;
    private final MappedClass mappedClass;

    MorphiaCodec(final Datastore datastore, final Mapper mapper, final MappedClass mappedClass, final ClassModel<T> classModel,
                 final CodecRegistry registry, final List<PropertyCodecProvider> propertyCodecProviders,
                 final DiscriminatorLookup discriminatorLookup) {
        super(classModel, registry, propertyCodecProviders, discriminatorLookup);
        this.datastore = datastore;
        this.mapper = mapper;
        this.mappedClass = mappedClass;
    }

    private MorphiaCodec(final Datastore datastore, final Mapper mapper, final MappedClass mappedClass, final ClassModel<T> classModel,
                         final CodecRegistry registry, final PropertyCodecRegistry propertyCodecRegistry,
                         final DiscriminatorLookup discriminatorLookup, final boolean specialized) {
        super(classModel, registry, propertyCodecRegistry, discriminatorLookup, new ConcurrentHashMap<>(), specialized);
        this.datastore = datastore;
        this.mapper = mapper;
        this.mappedClass = mappedClass;
    }

    @Override
    public T generateIdIfAbsentFromDocument(final T document) {
        if (!documentHasId(document)) {
            final MappedField mappedIdField = mappedClass.getMappedIdField();
            mappedIdField.setFieldValue(document, convert(new ObjectId(), mappedIdField.getType()));
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

    @Override
    protected <S> void encodeProperty(final BsonWriter writer,
                                      final T instance,
                                      final EncoderContext encoderContext,
                                      final PropertyModel<S> propertyModel) {
        final PropertyHandler handler = getPropertyHandler(getClassModel().getInstanceCreator(), propertyModel);
        if (handler != null) {
            handler.encodeProperty(writer, instance, encoderContext, propertyModel);
        } else {
            super.encodeProperty(writer, instance, encoderContext, propertyModel);
        }
    }

    @Override
    protected <S> void decodePropertyModel(final BsonReader reader,
                                           final DecoderContext decoderContext,
                                           final InstanceCreator<T> instanceCreator,
                                           final String name,
                                           final PropertyModel<S> propertyModel) {
        if (propertyModel != null) {
            final PropertyHandler handler = getPropertyHandler(instanceCreator, propertyModel);
            if (handler != null) {
                S value = handler.decodeProperty(reader, decoderContext, instanceCreator, name, propertyModel);
                instanceCreator.set(value, propertyModel);
            } else {
                final BsonReaderMark mark = reader.getMark();
                try {
                    super.decodePropertyModel(reader, decoderContext, instanceCreator, name, propertyModel);
                } catch (CodecConfigurationException e) {
                    mark.reset();
                    final Object value = mapper.getCodecRegistry().get(Object.class).decode(reader, decoderContext);
                    instanceCreator.set((S) convert(value, propertyModel.getTypeData().getType()), propertyModel);
                }
            }
        } else {
            reader.skipValue();
        }
    }

    private <S> PropertyHandler getPropertyHandler(final InstanceCreator<?> instanceCreator, final PropertyModel<S> propertyModel) {
        return instanceCreator instanceof MorphiaInstanceCreator ? ((MorphiaInstanceCreator) instanceCreator).getHandler(propertyModel)
                                                                 : null;

    }

    private <S> MappedField getMappedField(final PropertyModel<S> propertyModel) {
        final MappedField field = mappedClass.getMappedField(propertyModel.getName());
        return field != null ?  field : mappedClass.getMappedFieldByJavaField(propertyModel.getName());
    }

    public MappedClass getMappedClass() {
        return mappedClass;
    }

    @Override
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
                specialized = new MorphiaCodec<>(morphiaCodec.datastore,  morphiaCodec.mapper, new MappedClass(classModel, morphiaCodec
                                                                                                                            .mapper),
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
