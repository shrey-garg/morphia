package org.mongodb.morphia.mapping;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.mapping.codec.MorphiaCodecProvider;

import java.lang.annotation.Annotation;

class MorphiaShortCutProvider implements CodecProvider {
    private MorphiaCodecProvider codecProvider;

    MorphiaShortCutProvider(final MorphiaCodecProvider codecProvider) {
        this.codecProvider = codecProvider;
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return hasAnnotation(clazz, Entity.class) || hasAnnotation(clazz, Embedded.class)
               ? codecProvider.get(clazz, registry)
               : null;
    }

    private <T> boolean hasAnnotation(final Class<T> clazz, final Class<? extends Annotation> ann) {
        return clazz.getAnnotation(ann) != null;
    }
}
