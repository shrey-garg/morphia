package org.mongodb.morphia.mapping.codec;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.ClassModelBuilder;
import org.bson.codecs.pojo.Convention;
import org.bson.codecs.pojo.Conventions;
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
    private final Map<Class<?>, ClassModel<?>> classModels;
    private final Set<String> packages;
    private final Set<String> restrictedPackages = new HashSet<>(asList("java", "javax"));
    private final List<Convention> conventions;
    private final DiscriminatorLookup discriminatorLookup;
    private final List<PropertyCodecProvider> propertyCodecProviders;

    private MorphiaCodecProvider(final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages,
                                 final List<Convention> conventions, final List<PropertyCodecProvider> propertyCodecProviders) {
        this.classModels = classModels;
        this.packages = packages;
        this.conventions = conventions;
        this.discriminatorLookup = new DiscriminatorLookup(classModels, packages);
        this.propertyCodecProviders = propertyCodecProviders;
    }

    /**
     * Creates a Builder so classes or packages can be registered and configured before creating an immutable CodecProvider.
     *
     * @return the Builder
     * @see MorphiaCodecProvider.Builder#register(Class[])
     */
    public static MorphiaCodecProvider.Builder builder() {
        return new MorphiaCodecProvider.Builder();
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
     * A Builder for the MorphiaCodecProvider
     */
    public static final class Builder {
        private final Set<String> packages = new HashSet<>();
        private final Map<Class<?>, ClassModel<?>> classModels = new HashMap<>();
        private final List<Class<?>> clazzes = new ArrayList<>();
        private List<Convention> conventions = null;
        private final List<PropertyCodecProvider> propertyCodecProviders = new ArrayList<>();

        /**
         * Creates the MorphiaCodecProvider with the classes or packages that configured and registered.
         *
         * @return the Provider
         * @see #register(Class...)
         */
        public MorphiaCodecProvider build() {
            List<Convention> immutableConventions = conventions != null
                                                    ? Collections.unmodifiableList(new ArrayList<>(conventions))
                                                    : null;
            for (Class<?> clazz : clazzes) {
                if (!classModels.containsKey(clazz)) {
                    register(createClassModel(clazz, immutableConventions));
                }
            }
            return new MorphiaCodecProvider(classModels, packages, immutableConventions, propertyCodecProviders);
        }

        /**
         * Sets the conventions to use when creating {@code ClassModels} from classes or packages.
         *
         * @param conventions a list of conventions
         * @return this
         */
        public MorphiaCodecProvider.Builder conventions(final List<Convention> conventions) {
            this.conventions = notNull("conventions", conventions);
            return this;
        }

        /**
         * Registers a classes with the builder for inclusion in the Provider.
         *
         * <p>Note: Uses reflection for the property mapping. If no conventions are configured on the builder the
         * {@link Conventions#DEFAULT_CONVENTIONS} will be used.</p>
         *
         * @param classes the classes to register
         * @return this
         */
        public MorphiaCodecProvider.Builder register(final Class<?>... classes) {
            clazzes.addAll(asList(classes));
            return this;
        }

        /**
         * Registers classModels for inclusion in the Provider.
         *
         * @param classModels the classModels to register
         * @return this
         */
        public MorphiaCodecProvider.Builder register(final ClassModel<?>... classModels) {
            notNull("classModels", classModels);
            for (ClassModel<?> classModel : classModels) {
                this.classModels.put(classModel.getType(), classModel);
            }
            return this;
        }

        /**
         * Registers the packages of the given classes with the builder for inclusion in the Provider. This will allow classes in the
         * given packages to mapped for use with MorphiaCodecProvider.
         *
         * <p>Note: Uses reflection for the field mapping. If no conventions are configured on the builder the
         * {@link Conventions#DEFAULT_CONVENTIONS} will be used.</p>
         *
         * @param packageNames the package names to register
         * @return this
         */
        public MorphiaCodecProvider.Builder register(final String... packageNames) {
            packages.addAll(asList(notNull("packageNames", packageNames)));
            return this;
        }

        /**
         * Registers codec providers that receive the type parameters of properties for instances encoded and decoded
         * by a {@link PojoCodec} handled by this provider.
         *
         * <p>Note that you should prefer working with the {@link CodecRegistry}/{@link CodecProvider} hierarchy. Providers
         * should only be registered here if a codec needs to be created for custom container types like optionals and
         * collections. Support for types {@link Map} and {@link java.util.Collection} are built-in so explicitly handling
         * them is not necessary.
         * @param providers property codec providers to register
         * @return this
         * @since 3.6
         */
        public MorphiaCodecProvider.Builder register(final PropertyCodecProvider... providers) {
            propertyCodecProviders.addAll(asList(notNull("providers", providers)));
            return this;
        }

        private Builder() {
        }
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
