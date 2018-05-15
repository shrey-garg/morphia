package org.mongodb.morphia.mapping.codec;

import org.bson.BsonDocumentReader;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.DiscriminatorLookup;
import org.bson.codecs.pojo.FieldModel;
import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PojoCodec;
import org.bson.codecs.pojo.PojoCodecImpl;
import org.bson.codecs.pojo.PropertyCodecProvider;
import org.bson.codecs.pojo.PropertyCodecRegistry;
import org.bson.codecs.pojo.PropertyModel;
import org.mongodb.morphia.annotations.NotSaved;
import org.mongodb.morphia.annotations.PostLoad;
import org.mongodb.morphia.annotations.PostPersist;
import org.mongodb.morphia.annotations.PreLoad;
import org.mongodb.morphia.annotations.PrePersist;
import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class MorphiaCodec<T> extends PojoCodecImpl<T> {
    private static final Logger LOG = MorphiaLoggerFactory.get(MorphiaCodec.class);

    private final Mapper mapper;
    private final MappedClass mappedClass;
    private Map<String, FieldModel> fieldModelMap;

    MorphiaCodec(final Mapper mapper, final MappedClass mappedClass, final ClassModel<T> classModel, final CodecRegistry registry,
                 final List<PropertyCodecProvider> propertyCodecProviders, final DiscriminatorLookup discriminatorLookup) {
        super(classModel, registry, propertyCodecProviders, discriminatorLookup);
        this.mapper = mapper;
        this.mappedClass = mappedClass;
    }

    public MorphiaCodec(final Mapper mapper,
                        final MappedClass mappedClass,
                        final ClassModel<T> classModel,
                        final CodecRegistry registry,
                        final PropertyCodecRegistry propertyCodecRegistry, final DiscriminatorLookup discriminatorLookup, final boolean specialized) {
        super(classModel, registry, propertyCodecRegistry, discriminatorLookup, new ConcurrentHashMap<>(), specialized);
        this.mapper = mapper;
        this.mappedClass = mappedClass;
    }

    public MappedClass getMappedClass() {
        return mappedClass;
    }

    @Override
    public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        if (mappedClass.hasLifecycle(PrePersist.class)) {
            mappedClass.callLifecycleMethods(PrePersist.class, value, null, mapper);
        }
        if (mappedClass.hasLifecycle(PostPersist.class)) {
            final DocumentWriter documentWriter = new DocumentWriter();
            super.encode(documentWriter, value, encoderContext);
            final Document document = mappedClass.callLifecycleMethods(PostPersist.class, value, documentWriter.getRoot(), mapper);
            getRegistry().get(Document.class).encode(writer, document, encoderContext);
        } else {
            super.encode(writer, value, encoderContext);
        }
    }

    @Override
    protected <S> void encodeProperty(final BsonWriter writer,
                                      final T instance,
                                      final EncoderContext encoderContext,
                                      final PropertyModel<S> propertyModel) {
        if (!getMappedField(propertyModel.getWriteName()).hasAnnotation(NotSaved.class)) {
            super.encodeProperty(writer, instance, encoderContext, propertyModel);
        } else {
            LOG.info(format("Not saving %s#%s because it's marked with @NotSaved", getClassModel().getName(),
                propertyModel.getName()));
        }
    }

    private MappedField getMappedField(final String name) {
        return getMappedClass().getMappedField(name);
    }

    @Override
    public T decode(final BsonReader reader, final DecoderContext decoderContext) {
        T t;
        if (mappedClass.hasLifecycle(PreLoad.class)) {
            final InstanceCreator<T> instanceCreator = getClassModel().getInstanceCreator();
            t = instanceCreator.getInstance();
            Document document = getRegistry().get(Document.class).decode(reader, decoderContext);
            document = mappedClass.callLifecycleMethods(PreLoad.class, t, document, mapper);

            decodeProperties(new BsonDocumentReader(document.toBsonDocument(Document.class, mapper.getCodecRegistry())), decoderContext,
                instanceCreator);
        } else {
            t = super.decode(reader, decoderContext);
        }
        if (mappedClass.hasLifecycle(PostLoad.class)) {
            mappedClass.callLifecycleMethods(PostLoad.class, t, null, mapper);
        }

        return t;
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

        public <S> SpecializedMorphiaCodec(final MorphiaCodec morphiaCodec, final ClassModel<T> classModel) {

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

        @Override
        public void encode(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
            getSpecialized().encode(writer, value, encoderContext);
        }

        private MorphiaCodec<T> getSpecialized() {
            if(specialized == null) {
                specialized = new MorphiaCodec<T>(morphiaCodec.mapper, new MappedClass(classModel, morphiaCodec.mapper),
                    classModel, morphiaCodec.getRegistry(), morphiaCodec.getPropertyCodecRegistry(), morphiaCodec.getDiscriminatorLookup(),
                    true);
            }
            return specialized;
        }

        @Override
        public Class<T> getEncoderClass() {
            return classModel.getType();
        }
    }
}
