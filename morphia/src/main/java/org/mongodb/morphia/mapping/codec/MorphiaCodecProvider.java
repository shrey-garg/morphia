package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.DiscriminatorLookup;
import org.bson.codecs.pojo.PojoCodec;
import org.bson.codecs.pojo.PojoCodecImpl;
import org.bson.codecs.pojo.PropertyCodecProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;

public class MorphiaCodecProvider implements CodecProvider {
    private final Map<Class<?>, ClassModel<?>> classModels = new HashMap<>();
    private final Set<String> packages = new HashSet<>();
    private final Set<String> restrictedPackages = new HashSet<>(asList("java", "javax"));
    private final List<Convention> conventions;
    private final DiscriminatorLookup discriminatorLookup;
    private final List<PropertyCodecProvider> propertyCodecProviders = new ArrayList<>();

    public MorphiaCodecProvider(final List<Convention> conventions) {
        this.conventions = conventions;
        this.discriminatorLookup = new DiscriminatorLookup(this.classModels, this.packages);
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return getPojoCodec(clazz, registry);
    }

    @SuppressWarnings("unchecked")
    private <T> PojoCodec<T> getPojoCodec(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz.isEnum() || clazz.getPackage() == null) {
            return null;
        }
        for (String restrictedPackage : restrictedPackages) {
            if(clazz.getPackage().getName().startsWith(restrictedPackage + ".")) {
                return null;
            }
        }
        if (clazz.getPackage() != null && (packages.isEmpty() || packages.contains(clazz.getPackage().getName()))) {
            ClassModel<T> classModel = (ClassModel<T>) classModels.get(clazz);
            if (classModel == null) {
                classModel = createClassModel(clazz, conventions);
                discriminatorLookup.addClassModel(classModel);
            }
            return new MorphiaCodec<>(classModel, registry, propertyCodecProviders, discriminatorLookup);
        }
        return null;
    }

    /**
     * Registers codec providers that receive the type parameters of properties for instances encoded and decoded
     * by a {@link PojoCodec} handled by this provider.
     *
     * @param providers property codec providers to register
     */
    public void register(final PropertyCodecProvider... providers) {
        propertyCodecProviders.addAll(asList(notNull("providers", providers)));
    }

    /**
     * Registers the packages of the given classes with the builder for inclusion in the Provider. This will allow classes in the
     * given packages to mapped for use with MorphiaCodecProvider.
     *
     * @param packageNames the package names to register
     */
    public void addPackages(final String... packageNames) {
        packages.addAll(asList(notNull("packageNames", packageNames)));
    }

    private static <T> ClassModel<T> createClassModel(final Class<T> clazz, final List<Convention> conventions) {
        ClassModelBuilder<T> builder = ClassModel.builder(clazz);
        if (conventions != null) {
            builder.conventions(conventions);
        }
        return builder.build();
    }

    private class MorphiaCodec<T> extends PojoCodecImpl<T> {
        private MorphiaCodec(final ClassModel<T> classModel,
                            final CodecRegistry registry,
                            final List<PropertyCodecProvider> propertyCodecProviders,
                            final DiscriminatorLookup discriminatorLookup) {
            super(classModel, registry, propertyCodecProviders, discriminatorLookup);
        }
    }
}
