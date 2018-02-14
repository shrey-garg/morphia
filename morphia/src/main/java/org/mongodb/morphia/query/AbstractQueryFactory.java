package org.mongodb.morphia.query;

import com.mongodb.client.MongoCollection;
import org.mongodb.morphia.Datastore;

/**
 * An abstract implementation of {@link QueryFactory}.
 */
public abstract class AbstractQueryFactory implements QueryFactory {

    @Override
    public <T> Query<T> createQuery(final Datastore datastore, final MongoCollection<T> collection, final Class<T> type) {
        return createQuery(datastore, collection, type, null);
    }

    @Override
    public <T> Query<T> createQuery(final Datastore datastore) {
        return new QueryImpl<>(null, null, datastore);
    }
}
