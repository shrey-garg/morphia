package org.mongodb.morphia;


import org.bson.Document;
import org.mongodb.morphia.mapping.Mapper;

public class AbstractEntityInterceptor implements EntityInterceptor {

    @Override
    public void postLoad(final Object ent, final Document document, final Mapper mapper) {
    }

    @Override
    public void postPersist(final Object ent, final Document document, final Mapper mapper) {
    }

    @Override
    public void preLoad(final Object ent, final Document document, final Mapper mapper) {
    }

    @Override
    public void prePersist(final Object ent, final Document document, final Mapper mapper) {
    }

    @Override
    public void preSave(final Object ent, final Document document, final Mapper mapper) {
    }
}
