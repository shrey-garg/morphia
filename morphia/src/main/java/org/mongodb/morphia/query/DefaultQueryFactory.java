package org.mongodb.morphia.query;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.mongodb.morphia.Datastore;


/**
 * A default implementation of {@link QueryFactory}.
 */
public class DefaultQueryFactory extends AbstractQueryFactory {

    @Override
    public <T> Query<T> createQuery(final Datastore datastore,
                                    final MongoCollection<T> collection,
                                    final Class<T> type,
                                    final Document query) {

        final QueryImpl<T> item = new QueryImpl<>(type, collection, datastore);

        if (query != null) {
            item.setQueryDocument(query);
        }

        return item;
    }

}
