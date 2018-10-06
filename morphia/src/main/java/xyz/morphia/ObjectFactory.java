package xyz.morphia;

import org.bson.Document;
import xyz.morphia.mapping.MappedField;
import xyz.morphia.mapping.Mapper;

/**
 * The ObjectFactory is used by morphia to create instances of classes which can be customized to fit a particular applications needs.
 */
public interface ObjectFactory {
    /**
     * Creates an instance of the given class.
     *
     * @param clazz type class to instantiate
     * @param <T>   the type of the entity
     * @return the new instance
     */
    <T> T createInstance(Class<T> clazz);

    /**
     * Creates an instance of the class defined in the {@link Mapper#CLASS_NAME_FIELDNAME} field in the document passed in.  If that
     * field is missing, the given Class is used instead.
     *
     * @param clazz type class to instantiate
     * @param document the state to populate the new instance with
     * @param <T>   the type of the entity
     * @return the new instance
     */
    <T> T createInstance(Class<T> clazz, Document document);

    /**
     * Creates an instance of the class defined in the {@link Mapper#CLASS_NAME_FIELDNAME} field in the document passed in.  If that
     * field is missing, morphia attempts to the MappedField to determine which concrete class to instantiate.
     *
     * @param mapper the Mapper to use
     * @param mf     the MappedField to consult when creating the instance
     * @param document  the state to populate the new instance with
     * @return the new instance
     */
    Object createInstance(Mapper mapper, MappedField mf, Document document);
}
