package org.mongodb.morphia.mapping;

import org.bson.codecs.pojo.InstanceCreator;
import org.bson.codecs.pojo.PropertyModel;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.mapping.experimental.PropertyHandler;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Defines a Morphia-specific {@code InstanceCreator}
 * @param <T>
 */
public interface MorphiaInstanceCreator<T> extends InstanceCreator<T> {

    /**
     * Defers the creation of an instance using the passed lambda
     *
     * @param function the implementation of the deferral
     */
    void defer(BiConsumer<Datastore, Map<Object, Object>> function);

    /**
     * @param propertyModel the model property
     * @param <S> type type of the property
     * @return the handler for the property or null if there is no handler
     */
    <S> PropertyHandler getHandler(PropertyModel<S> propertyModel);
}
