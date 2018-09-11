package org.mongodb.morphia.query;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoCursor;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.mapping.Mapper;

/**
 * Defines an Iterator across the Key values for a given type.
 *
 * @param <T> the entity type
 */
public class MorphiaKeyIterator<T> implements MongoCursor<Key<T>> {
    private final MongoCursor<T> cursor;
    private final Mapper mapper;

    /**
     * Create
     *
     * @param cursor the cursor to use
     * @param mapper the Mapper to use
     */
    MorphiaKeyIterator(final MongoCursor<T> cursor, final Mapper mapper) {
        this.cursor = cursor;
        this.mapper = mapper;
    }

    @Override
    public void close() {
        cursor.close();
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public Key<T> next() {
        return mapper.getKey(cursor.next());
    }

    @Override
    public Key<T> tryNext() {
        final T entity = cursor.tryNext();
        return entity != null ? mapper.getKey(entity) : null;
    }

    @Override
    public ServerCursor getServerCursor() {
        return cursor.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return cursor.getServerAddress();
    }

    @Override
    public void remove() {
        cursor.remove();
    }
}
