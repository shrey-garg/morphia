package org.mongodb.morphia.mapping.experimental;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PropertyModel;
import org.bson.codecs.pojo.TypeData;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.mapping.MappedClass;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Defines a specialized handler for a property.  This is used to customize how Morphia reads and writes value for a property.  When a
 * handler is defined, the standard serialization process is skipped.  Implementations of PropertyHandler behave similarly to Codecs
 * defined in the Java driver.
 *
 *
 * @see org.bson.codecs.Codec
 */
public abstract class PropertyHandler {
    private final Field field;
    private final String propertyName;
    private final TypeData typeData;
    private MappedClass mappedClass;
    private Datastore datastore;

    /**
     * Creates a new handler
     *
     * @param datastore the {@code Datastore} to use
     * @param field the field to read and write
     * @param propertyName the name of the property
     * @param typeData the type data for the field
     */
    public PropertyHandler(final Datastore datastore, final Field field, final String propertyName, final TypeData typeData) {
        this.datastore = datastore;
        this.propertyName = propertyName;
        this.field = field;
        this.typeData = typeData;
    }

    /**
     * @return the Datastore
     */
    public Datastore getDatastore() {
        return datastore;
    }

    /**
     * @return the field
     */
    public Field getField() {
        return field;
    }

    protected MappedClass getFieldMappedClass() {
        if (mappedClass == null) {
            Class<?> type = typeData.getTypeParameters().size() == 0
                            ? typeData.getType()
                            : ((TypeData) typeData.getTypeParameters().get(typeData.getTypeParameters().size() - 1)).getType();
            if (type.isArray()) {
                type = type.getComponentType();
            }
            mappedClass = datastore.getMapper().getMappedClass(type);
        }
        return mappedClass;
    }

    /**
     * @return the property name
     */
    public String getPropertyName() {
        return propertyName;
    }

    /**
     * @return the type data
     */
    public TypeData getTypeData() {
        return typeData;
    }

    /**
     * Sets the field on an instance with the given value
     * @param instance the instance to update
     * @param propertyModel the PropertyModel for the field
     * @param value the value to set
     * @param datastore the datastore
     * @param entityCache the entity cache in use
     * @param <S> the type of the value
     * @param <E> the type of the instance
     */
    public abstract <S, E> void set(E instance, PropertyModel<S> propertyModel, S value, Datastore datastore,
                                    Map<Object, Object> entityCache);

    /**
     * Decodes a property value
     *
     * @param reader the BsonReader being used
     * @param decoderContext the decoding context
     * @param instanceCreator the instance creator
     * @param name the name of the field
     * @param propertyModel the model for the field
     * @param <T> the type of the instance
     * @param <S> type of the field
     *
     * @return the decoded property value
     */
    public abstract <T, S> S decodeProperty(BsonReader reader,
                                            DecoderContext decoderContext,
                                            InstanceCreator<T> instanceCreator,
                                            String name,
                                            PropertyModel<S> propertyModel);

    /**
     * Encodes a property for storage in the database.
     *
     * @param writer the writer to use
     * @param instance the instance being written out
     * @param encoderContext the encoding context
     * @param propertyModel the model of the property
     * @param <S> the type of the property
     * @param <T> the type of the instance
     */
    public abstract <S, T> void encodeProperty(BsonWriter writer,
                                               T instance,
                                               EncoderContext encoderContext,
                                               PropertyModel<S> propertyModel);

    /**
     * Encodes a value
     *
     * @param value the value to encode
     * @param <S> the type of the value
     * @return the encoded value
     */
    public abstract <S> Object encodeValue(S value);
}
