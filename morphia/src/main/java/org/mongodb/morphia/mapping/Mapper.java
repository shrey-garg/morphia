/*
 * Copyright (C) 2010 Olafur Gauti Gudmundsson Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless
 * required
 * by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations
 * under the License.
 */


package org.mongodb.morphia.mapping;


import org.bson.Document;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.mongodb.morphia.EntityInterceptor;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;
import org.mongodb.morphia.mapping.codec.DocumentWriter;
import org.mongodb.morphia.mapping.codec.MorphiaCodec;
import org.mongodb.morphia.mapping.codec.MorphiaCodecProvider;
import org.mongodb.morphia.mapping.codec.MorphiaTypesCodecProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


/**
 * <p>This is the heart of Morphia and takes care of mapping from/to POJOs/Documents<p> <p>This class is thread-safe and keeps various
 * "cached" data which should speed up processing.</p>
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class Mapper {
    /**
     * The @{@link org.mongodb.morphia.annotations.Id} field name that is stored with mongodb.
     */
    public static final String ID_KEY = "_id";
    /**
     * Special name that can never be used. Used as default for some fields to indicate default state.
     */
    public static final String IGNORED_FIELDNAME = ".";
    /**
     * Special field used by morphia to support various possibly loading issues; will be replaced when discriminators are implemented to
     * support polymorphism
     */
    public static final String CLASS_NAME_FIELDNAME = "className";
    private static final Logger LOG = MorphiaLoggerFactory.get(Mapper.class);
    /**
     * Set of classes that registered by this mapper
     */
    private final Map<String, MappedClass> mappedClasses = new HashMap<>();
    private final Map<String, Set<MappedClass>> mappedClassesByCollection = new HashMap<>();

    //EntityInterceptors; these are called after EntityListeners and lifecycle methods on an Entity, for all Entities
    private final List<EntityInterceptor> interceptors = new LinkedList<>();

    //A general cache of instances of classes; used by MappedClass for EntityListener(s)
    private final Map<Class, Object> instanceCache = new ConcurrentHashMap();
    private CodecRegistry codecRegistry;
    private MapperOptions opts;
    private MorphiaCodecProvider codecProvider;

    public Mapper(final CodecRegistry codecRegistry) {
        this(codecRegistry, new MapperOptions());
    }

    /**
     * Creates a Mapper with the given options.
     *
     * @param opts the options to use
     */
    public Mapper(final CodecRegistry codecRegistry, final MapperOptions opts) {

        this.opts = opts;
        codecProvider = new MorphiaCodecProvider(this, singletonList(new MorphiaConvention(opts)));
        final MorphiaTypesCodecProvider typesCodecProvider = new MorphiaTypesCodecProvider(this);

        this.codecRegistry = fromRegistries(
            codecRegistry,
            fromProviders(typesCodecProvider, codecProvider));
    }

    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    /**
     * Adds an {@link EntityInterceptor}
     *
     * @param ei the interceptor to add
     */
    public void addInterceptor(final EntityInterceptor ei) {
        interceptors.add(ei);
    }

    /**
     * Finds any subtypes for the given MappedClass.
     *
     * @param mc the parent type
     * @return the list of subtypes
     * @since 1.3
     */
    public List<MappedClass> getSubTypes(final MappedClass mc) {
        List<MappedClass> subtypes = new ArrayList<>();
        for (MappedClass mappedClass : getMappedClasses()) {
            if (mappedClass.isSubType(mc)) {
                subtypes.add(mappedClass);
            }
        }

        return subtypes;
    }

    /**
     * @return collection of MappedClasses
     */
    public Collection<MappedClass> getMappedClasses() {
        return new ArrayList<>(mappedClasses.values());
    }

    /**
     * Looks up the class mapped to a named collection.
     *
     * @param collection the collection name
     * @return the Class mapped to this collection name
     */
    public <T> Class<T> getClassFromCollection(final String collection) {
        final Set<MappedClass> mcs = mappedClassesByCollection.get(collection);
        if (mcs == null || mcs.isEmpty()) {
            throw new MappingException(format("The collection '%s' is not mapped to a java class.", collection));
        }
        if (mcs.size() > 1) {
            if (LOG.isInfoEnabled()) {
                LOG.info(format("Found more than one class mapped to collection '%s'%s", collection, mcs));
            }
        }
        return (Class<T>) mcs.iterator().next().getClazz();
    }

    /**
     * Gets the mapped collection for an object instance or Class reference.
     *
     * @param classModel the classModel to process
     * @return the collection name
     */
    public String getCollectionName(final ClassModel classModel) {
        if (classModel == null) {
            throw new IllegalArgumentException();
        }

        return getCollectionName(classModel.getType());
    }

    public String getCollectionName(final Class type) {
        final Entity entityAn = (Entity) type.getAnnotation(Entity.class);
        if (entityAn == null || entityAn.value().equals(Mapper.IGNORED_FIELDNAME)) {
            return getOptions().isUseLowerCaseCollectionNames()
                   ? type.getSimpleName().toLowerCase()
                   : type.getSimpleName();
        }
        return entityAn.value();
    }

    /**
     * @return the options used by this Mapper
     */
    public MapperOptions getOptions() {
        return opts;
    }

    /**
     * Sets the options this Mapper should use
     *
     * @param options the options to use
     */
    public void setOptions(final MapperOptions options) {
        opts = options;
    }

    /**
     * @return the cache of instances
     */
    Map<Class, Object> getInstanceCache() {
        return instanceCache;
    }

    /**
     * Gets list of {@link EntityInterceptor}s
     *
     * @return the Interceptors
     */
    Collection<EntityInterceptor> getInterceptors() {
        return interceptors;
    }

    /**
     * Gets the Key for an entity
     *
     * @param entity the entity to process
     * @param <T>    the type of the entity
     * @return the Key
     */
    public <T> Key<T> getKey(final T entity) {
        if (entity instanceof Key) {
            return (Key<T>) entity;
        }

        final Object id = getId(entity);
        final Class<T> aClass = (Class<T>) entity.getClass();
        return id == null ? null : new Key<>(aClass, getCollectionName(aClass), id);
    }

    /**
     * Gets the ID value for an entity
     *
     * @param entity the entity to process
     * @return the ID value
     */
    public Object getId(final Object entity) {
        if (entity == null) {
            return null;
        }
        try {
            final MappedClass mappedClass = getMappedClass(entity.getClass());
            final MappedField idField = mappedClass.getIdField();
            return idField.getFieldValue(entity);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Gets the {@link MappedClass} for the object (type). If it isn't mapped, create a new class and cache it (without validating).
     *
     * @param obj the object to process
     * @return the MappedClass for the object given
     */
    public MappedClass getMappedClass(final Object obj) {
        if (obj == null) {
            return null;
        }

        Class type = (obj instanceof Class) ? (Class) obj : obj.getClass();

        MappedClass mc = mappedClasses.get(type.getName());
        if (mc == null) {
            mc = addMappedClass(type, false);
        }
        return mc;
    }

    /**
     * Creates a MappedClass and validates it.
     *
     * @param c        the Class to map
     * @param validate if true, validate the mapping
     * @return the MappedClass for the given Class
     */
    public MappedClass addMappedClass(final Class c, final boolean validate) {
        MappedClass mappedClass = mappedClasses.get(c.getName());
        if (mappedClass == null) {
            final MorphiaCodec codec = (MorphiaCodec) getCodecProvider().get(c, codecRegistry);
            if(codec != null) {
                return addMappedClass(codec.getMappedClass(), validate);
            }
        }
        return mappedClass;
    }

    public CodecProvider getCodecProvider() {
        return codecProvider;
    }

    private MappedClass addMappedClass(final MappedClass mc, final boolean validate) {
        if (validate && !mc.isInterface()) {
            mc.validate(this);
        }

        mappedClasses.put(mc.getClazz().getName(), mc);

        Set<MappedClass> mcs = mappedClassesByCollection.get(mc.getCollectionName());
        if (mcs == null) {
            mcs = new CopyOnWriteArraySet<>();
            final Set<MappedClass> temp = mappedClassesByCollection.putIfAbsent(mc.getCollectionName(), mcs);
            if (temp != null) {
                mcs = temp;
            }
        }

        mcs.add(mc);

        return mc;
    }

    /**
     * Gets the Key for an entity and a specific collection
     *
     * @param entity     the entity to process
     * @param collection the collection to use in the Key rather than the mapped collection as defined on the entity's class
     * @param <T>        the type of the entity
     * @return the Key
     */
    public <T> Key<T> getKey(final T entity, final String collection) {
        if (entity instanceof Key) {
            return (Key<T>) entity;
        }

        final Object id = getId(entity);
        final Class<T> aClass = (Class<T>) entity.getClass();
        return id == null ? null : new Key<>(aClass, collection, id);
    }

    /**
     * Updates the collection value on a Key with the mapped value on the Key's type Class
     *
     * @param key the Key to update
     */
    public void coerceCollection(final Key key) {
        if (key.getCollection() == null && key.getType() == null) {
            throw new IllegalStateException("Key is invalid! " + toString());
        } else if (key.getCollection() == null) {
            key.setCollection(getMappedClass(key.getType()).getCollectionName());
        }

    }

    /**
     * Maps a set of classes
     *
     * @param entityClasses the classes to map
     */
    public void map(final Class... entityClasses) {
        for (final Class entityClass : entityClasses) {
            addMappedClass(entityClass, true);
        }
    }

    public void map(final Set<Class> entityClasses) {
        if (entityClasses != null && !entityClasses.isEmpty()) {
            for (final Class entityClass : entityClasses) {
                addMappedClass(entityClass, true);
            }
        }
    }

    /**
     * Maps all the classes found in the package to which the given class belongs.
     *
     * @param clazz the class to use when trying to find others to map
     */
    public void mapPackageFromClass(final Class clazz) {
        mapPackage(clazz.getPackage().getName());
    }

    /**
     * Tries to map all classes in the package specified. Fails if one of the classes is not valid for mapping.
     *
     * @param packageName the name of the package to process
     */
    public void mapPackage(final String packageName) {
        codecProvider.addPackages(packageName);
    }

    public <T> Document toDocument(final T entity) {
        final Class<T> aClass = (Class<T>) entity.getClass();
        final DocumentWriter writer = new DocumentWriter();
        codecRegistry.get(aClass).encode(writer, entity,
            EncoderContext.builder()
                          .isEncodingCollectibleDocument(true)
                          .build());

        return writer.getRoot();
    }

}
