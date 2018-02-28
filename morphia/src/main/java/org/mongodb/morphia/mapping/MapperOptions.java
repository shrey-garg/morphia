package org.mongodb.morphia.mapping;


import org.mongodb.morphia.ObjectFactory;
import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;
import org.mongodb.morphia.mapping.cache.DefaultEntityCacheFactory;
import org.mongodb.morphia.mapping.cache.EntityCacheFactory;

/**
 * Options to control mapping behavior.
 */
public class MapperOptions {
    private static final Logger LOG = MorphiaLoggerFactory.get(MapperOptions.class);
    private boolean ignoreFinals; //ignore final fields.
    private boolean storeNulls;
    private boolean storeEmpties;
    private boolean useLowerCaseCollectionNames;
    private boolean mapSubPackages = false;
    private ObjectFactory objectFactory = new DefaultCreator();
    private EntityCacheFactory cacheFactory = new DefaultEntityCacheFactory();

    /**
     * Creates a default options instance.
     */
    public MapperOptions() {
    }

    /**
     * Copy Constructor
     *
     * @param options the MapperOptions to copy
     */
    public MapperOptions(final MapperOptions options) {
        setIgnoreFinals(options.isIgnoreFinals());
        setStoreNulls(options.isStoreNulls());
        setStoreEmpties(options.isStoreEmpties());
        setUseLowerCaseCollectionNames(options.isUseLowerCaseCollectionNames());
        setObjectFactory(options.getObjectFactory());
        setCacheFactory(options.getCacheFactory());
    }

    /**
     * @return the factory to create an EntityCache
     */
    public EntityCacheFactory getCacheFactory() {
        return cacheFactory;
    }

    /**
     * Sets the factory to create an EntityCache
     *
     * @param cacheFactory the factory
     */
    public void setCacheFactory(final EntityCacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
    }

    /**
     * @return the factory to use when creating new instances
     */
    public ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    /**
     * Sets the ObjectFactory to use when instantiating entity classes.  The default factory is a simple reflection based factory but this
     * could be used, e.g., to provide a Guice-based factory such as what morphia-guice provides.
     *
     * @param objectFactory the factory to use
     */
    public void setObjectFactory(final ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    /**
     * @return true if Morphia should cache name -> Class lookups
     */
    public boolean isCacheClassLookups() {
        return false;
    }

    /**
     * @return true if Morphia should ignore final fields
     */
    public boolean isIgnoreFinals() {
        return ignoreFinals;
    }

    /**
     * Controls if final fields are stored.
     *
     * @param ignoreFinals true if Morphia should ignore final fields
     */
    public void setIgnoreFinals(final boolean ignoreFinals) {
        this.ignoreFinals = ignoreFinals;
    }

    /**
     * @return true if Morphia should store empty values for lists/maps/sets/arrays
     */
    public boolean isStoreEmpties() {
        return storeEmpties;
    }

    /**
     * Controls if Morphia should store empty values for lists/maps/sets/arrays
     *
     * @param storeEmpties true if Morphia should store empty values for lists/maps/sets/arrays
     */
    public void setStoreEmpties(final boolean storeEmpties) {
        this.storeEmpties = storeEmpties;
    }

    /**
     * @return true if Morphia should store null values
     */
    public boolean isStoreNulls() {
        return storeNulls;
    }

    /**
     * Controls if null are stored.
     *
     * @param storeNulls true if Morphia should store null values
     */
    public void setStoreNulls(final boolean storeNulls) {
        this.storeNulls = storeNulls;
    }

    /**
     * @return true if Morphia should use lower case values when calculating collection names
     */
    public boolean isUseLowerCaseCollectionNames() {
        return useLowerCaseCollectionNames;
    }

    /**
     * Controls if default entity collection name should be lowercase.
     *
     * @param useLowerCaseCollectionNames true if Morphia should use lower case values when calculating collection names
     */
    public void setUseLowerCaseCollectionNames(final boolean useLowerCaseCollectionNames) {
        this.useLowerCaseCollectionNames = useLowerCaseCollectionNames;
    }

    /**
     * @return true if Morphia should map classes from the sub-packages as well
     */
    public boolean isMapSubPackages() {
        return mapSubPackages;
    }

    /**
     * Controls if classes from sub-packages should be mapped.
     * @param mapSubPackages true if Morphia should map classes from the sub-packages as well
     */
    public void setMapSubPackages(final boolean mapSubPackages) {
        this.mapSubPackages = mapSubPackages;
    }
}
