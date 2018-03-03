package org.mongodb.morphia;


import org.bson.Document;
import org.mongodb.morphia.annotations.PostLoad;
import org.mongodb.morphia.annotations.PostPersist;
import org.mongodb.morphia.annotations.PreLoad;
import org.mongodb.morphia.annotations.PreSave;
import org.mongodb.morphia.mapping.Mapper;


/**
 * Interface for intercepting @Entity lifecycle events
 */
public interface EntityInterceptor {
    /**
     * @param ent      the entity being processed
     * @param document the Document form of the entity
     * @param mapper   the Mapper being used
     * @see PostLoad
     */
    default void postLoad(Object ent, Document document, Mapper mapper) {

    }

    /**
     * @param ent      the entity being processed
     * @param document the Document form of the entity
     * @param mapper   the Mapper being used
     * @see PostPersist
     */
    default void postPersist(Object ent, Document document, Mapper mapper) {

    }

    /**
     * @param ent      the entity being processed
     * @param document the Document form of the entity
     * @param mapper   the Mapper being used
     * @see PreLoad
     */
    default void preLoad(Object ent, Document document, Mapper mapper) {

    }

    /**
     * @param ent      the entity being processed
     * @param document the Document form of the entity
     * @param mapper   the Mapper being used
     * @see PostPersist
     */
    default void prePersist(Object ent, Document document, Mapper mapper) {

    }

    /**
     * @param ent      the entity being processed
     * @param document the Document form of the entity
     * @param mapper   the Mapper being used
     * @see PreSave
     */
    default void preSave(Object ent, Document document, Mapper mapper) {

    }
}
