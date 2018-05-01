package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.DiscriminatorLookup;
import org.bson.codecs.pojo.PojoCodec;
import org.bson.codecs.pojo.PropertyCodecProvider;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;

public class MorphiaCodecProvider implements CodecProvider {
    private final Map<Class<?>, ClassModel<?>> classModels = new HashMap<>();
    private final Map<Class<?>, MorphiaCodec<?>> codecs = new HashMap<>();
    private final Set<String> packages = new HashSet<>();
    private final Set<String> restrictedPackages = new HashSet<>(asList("java", "javax"));
    private final Mapper mapper;
    private final List<Convention> conventions;
    private final DiscriminatorLookup discriminatorLookup;
    private final List<PropertyCodecProvider> propertyCodecProviders = new ArrayList<>();

    public MorphiaCodecProvider(final Mapper mapper, final List<Convention> conventions) {
        this.mapper = mapper;
        this.conventions = conventions;
        this.discriminatorLookup = new DiscriminatorLookup(this.classModels, this.packages);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz.isEnum() || clazz.getPackage() == null) {
            return null;
        }
        if (!clazz.isInterface()) {
            for (String restrictedPackage : restrictedPackages) {
                if(clazz.getPackage().getName().startsWith(restrictedPackage + ".")) {
                    return null;
                }
            }
        }
        if (clazz.getPackage() != null && (packages.isEmpty() || packages.contains(clazz.getPackage().getName()))) {
            MorphiaCodec<?> codec = codecs.get(clazz);
            if (codec == null) {
                ClassModel<T> classModel = createClassModel(clazz, conventions);
                discriminatorLookup.addClassModel(classModel);
                codec = new MorphiaCodec<>(mapper, new MappedClass(classModel, mapper), classModel, registry,
                    propertyCodecProviders, discriminatorLookup);
            }

            return (Codec<T>) codec;
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

}
