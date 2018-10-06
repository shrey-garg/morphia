package xyz.morphia.mapping;


import xyz.morphia.ObjectFactory;
import xyz.morphia.annotations.Reference;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;

/**
 * Options to control mapping behavior.
 */
public class MapperOptions {
    private List<Class<? extends Annotation>> propertyHandlers = new ArrayList<>();
    private boolean ignoreFinals; //ignore final fields.
    private boolean storeNulls;
    private boolean storeEmpties;
    private boolean useLowerCaseCollectionNames;
    private boolean mapSubPackages;
    private ObjectFactory objectFactory = new DefaultCreator();

    /**
     * Creates a default options instance.
     */
    public MapperOptions() {
        addPropertyHandler(Reference.class);
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
        propertyHandlers.addAll(options.getPropertyHandlers());
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

    /**
     * Adds an annotation to be handled by property handle.
     *
     * This method is experimental and its signature may change.
     *
     * @param annotation the
     */
    public void addPropertyHandler(final Class<? extends Annotation> annotation) {
        propertyHandlers.add(annotation);
    }

    /**
     * @return the annotations to be used by {@code PropertyHandler}s
     */
    public List<Class<? extends Annotation>> getPropertyHandlers() {
        return propertyHandlers;
    }
}
