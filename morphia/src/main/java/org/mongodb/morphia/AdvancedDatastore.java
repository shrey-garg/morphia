package org.mongodb.morphia;

import com.mongodb.DBRef;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneOptions;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.mongodb.morphia.aggregation.AggregationPipeline;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import java.util.List;

/**
 * This interface exposes advanced {@link Datastore} features, like interacting with Document and low-level options. It implements matching
 * methods from the {@code Datastore} interface but with a specified kind (collection name), or raw types (Document).
 *
 * @author ScottHernandez
 */
public interface AdvancedDatastore extends Datastore {

    /**
     * Returns an {@link AggregationPipeline} bound to the given collection and class.
     *
     * @param collection the collection to query
     * @param clazz      The class to create aggregation against
     * @return the aggregation pipeline
     */
    AggregationPipeline createAggregation(String collection, Class<?> clazz);

    /**
     * @param <T>        The type of the entity
     * @param collection the collection to query
     * @param clazz      the class of objects to be returned
     * @return Query for the specified class clazz
     */
    <T> Query<T> createQuery(String collection, Class<T> clazz);

    /**
     * @param <T>   The type of the entity
     * @param clazz the class of objects to be returned
     * @param q     the query which will be passed to a {@link org.mongodb.morphia.query.QueryFactory}
     * @return Query for the specified class clazz
     */
    <T> Query<T> createQuery(Class<T> clazz, Document q);

    /**
     * @param <T>        The type of the entity
     * @param collection the collection to query
     * @param clazz      the class of objects to be returned
     * @param q          the query which will be passed to a {@link org.mongodb.morphia.query.QueryFactory}
     * @return Query for the specified class clazz
     */
    <T> Query<T> createQuery(String collection, Class<T> clazz, Document q);

    /**
     * Creates an UpdateOperations instance for the given type.
     *
     * @param <T>        The type of the entity
     * @param type       The type of the entity
     * @param operations The operations to perform
     * @return the UpdateOperations instance
     */
    <T> UpdateOperations<T> createUpdateOperations(Class<T> type, Document operations);

    /**
     * Deletes an entity of the given type T, with the given {@code id}, from the collection with the name in the {@code kind} param.
     * Validates the {@code id}, checking it's the correct type for an ID for entities of type {@code T}. The entity type {@code clazz} is
     * used only for validation, not for filtering, therefore if you have entities of different types in the same collection ({@code
     * kind}),
     * this method will delete any entity with the given {@code id}, regardless of its type.
     *
     * @param kind  the collection name
     * @param clazz the Class of the entity to delete
     * @param id    the value of the ID
     * @param <T>   the entity type
     * @param <V>   is the type of the ID, for example ObjectId
     * @return the result of this delete operation.
     */
    <T, V> DeleteResult delete(String kind, Class<T> clazz, V id);

    /**
     * Deletes an entity of the given type T, with the given {@code id}, from the collection with the name in the {@code kind} param.
     * Validates the {@code id}, checking it's the correct type for an ID for entities of type {@code T}. The entity type {@code clazz} is
     * used only for validation, not for filtering, therefore if you have entities of different types in the same collection ({@code
     * kind}),
     * this method will delete any entity with the given {@code id}, regardless of its type.
     *
     * @param kind    the collection name
     * @param clazz   the Class of the entity to delete
     * @param id      the value of the ID
     * @param options the options to use when deleting
     * @param <T>     the entity type
     * @param <V>     is the type of the ID, for example ObjectId
     * @return the result of this delete operation.
     * @since 1.3
     */
    <T, V> DeleteResult delete(String kind, Class<T> clazz, V id, DeleteOptions options);

    /**
     * Ensures (creating if necessary) the indexes found during class mapping (using {@code @Indexed, @Indexes)} on the given collection
     * name.
     *
     * @param collection the collection to update
     * @param clazz      the class from which to get the index definitions
     * @param <T>        the type to index
     */
    <T> void ensureIndexes(String collection, Class<T> clazz);

    /**
     * Ensures (creating if necessary) the indexes found during class mapping (using {@code @Indexed, @Indexes)} on the given collection
     * name, possibly in the background
     *
     * @param collection the collection to update
     * @param clazz      the class from which to get the index definitions
     * @param background if true, the index will be built in the background.  If false, the method will block until the index is created.
     * @param <T>        the type to index
     */
    <T> void ensureIndexes(String collection, Class<T> clazz, boolean background);

    /**
     * Checks that an entity exists for the given key or entity
     *
     * @param keyOrEntity    the value to check for
     * @param readPreference Uses the supplied ReadPreference for the check.  If readPreference is null the preference is taken from the
     *                       annotation or uses the default preference.
     * @return the key if the entity exists
     * @see #exists(Object)
     */
    Key<?> exists(Object keyOrEntity, ReadPreference readPreference);

    /**
     * Find all instances by type in a different collection than what is mapped on the class given.
     *
     * @param collection the collection to query against
     * @param clazz      the class to use for mapping the results
     * @param <T>        the type to query
     * @return the query
     */
    <T> Query<T> find(String collection, Class<T> clazz);

    /**
     * Find all instances by type in a different collection than what is mapped on the class given skipping some documents and returning a
     * fixed number of the remaining.
     *
     * @param collection the collection to query against
     * @param clazz      the class to use for mapping the results
     * @param property   the document property to query against
     * @param value      the value to check for
     * @param offset     the number of results to skip
     * @param size       the maximum number of results to return
     * @param <T>        the type to query
     * @param <V>        the type to filter value
     * @return the query
     */
    <T, V> Query<T> find(String collection, Class<T> clazz, String property, V value, int offset, int size);

    /**
     * Find the given entity (by collectionName/id);
     *
     * @param clazz the class to use for mapping
     * @param ref   the DBRef to use when querying
     * @param <T>   the type to fetch
     * @return the entity referenced in the DBRef.  May be null.
     */
    <T> T get(Class<T> clazz, DBRef ref);

    /**
     * Finds an entity in the named collection whose id matches the value given.
     *
     * @param collection the collection to query
     * @param clazz      the class to use for mapping
     * @param id         the ID to query
     * @param <T>        the type to fetch
     * @param <V>        the type of the ID
     * @return the entity with the id.  May be null.
     */
    <T, V> T get(String collection, Class<T> clazz, V id);

    /**
     * Gets the count this collection
     *
     * @param collection the collection to count
     * @return the collection size
     */
    long getCount(String collection);

    /**
     * Inserts an entity in to the mapped collection.
     *
     * @param entity the entity to insert
     * @param <T>    the type of the entity
     * @return the new key of the inserted entity
     */
    <T> Key<T> insert(T entity);

    /**
     * Inserts an entity in to the mapped collection.
     *
     * @param entity  the entity to insert
     * @param options the options to apply to the insert operation
     * @param <T>     the type of the entity
     * @return the new key of the inserted entity
     * @since 1.3
     */
    <T> Key<T> insert(T entity, InsertOneOptions options);

    /**
     * Inserts an entity in to the named collection.
     *
     * @param collection the collection to update
     * @param entity     the entity to insert
     * @param <T>        the type of the entity
     * @return the new key of the inserted entity
     */
    <T> Key<T> insert(String collection, T entity);

    /**
     * Inserts an entity in to the named collection.
     *
     * @param collection the collection to update
     * @param entity     the entity to insert
     * @param options    the options to apply to the insert operation
     * @param <T>        the type of the entity
     * @return the new key of the inserted entity
     * @since 1.3
     */
    <T> Key<T> insert(String collection, T entity, InsertOneOptions options);

    /**
     * Inserts entities in to the mapped collection.
     *
     * @param entities the entities to insert
     * @param <T>      the type of the entities
     * @return the new keys of the inserted entities
     */
    <T> List<Key<T>> insert(List<T> entities);

    /**
     * Inserts entities in to the mapped collection.
     *
     * @param entities the entities to insert
     * @param options  the options to apply to the insert operation
     * @param <T>      the type of the entity
     * @return the new keys of the inserted entities
     * @since 1.3
     */
    <T> List<Key<T>> insert(List<T> entities, InsertManyOptions options);

    /**
     * Inserts an entity in to the named collection.
     *
     * @param collection the collection to update
     * @param entities   the entities to insert
     * @param <T>        the type of the entity
     * @return the new keys of the inserted entities
     * @see WriteConcern
     */
    <T> List<Key<T>> insert(String collection, List<T> entities);

    /**
     * Inserts entities in to the named collection.
     *
     * @param collection the collection to update
     * @param entities   the entities to insert
     * @param options    the options to apply to the insert operation
     * @param <T>        the type of the entity
     * @return the new keys of the inserted entities
     * @since 1.3
     */
    <T> List<Key<T>> insert(String collection, List<T> entities, InsertManyOptions options);

    /**
     * Returns a new query based on the example object
     *
     * @param collection the collection to query
     * @param example    the example entity to use when building the query
     * @param <T>        the type of the entity
     * @return the query
     */
    <T> Query<T> queryByExample(String collection, T example);

    /**
     * Saves an entity in to the named collection.
     *
     * @param collection the collection to update
     * @param entity     the entity to save
     * @param <T>        the type of the entity
     * @return the new key of the inserted entity
     */
    <T> Key<T> save(String collection, T entity);

    /**
     * Saves an entity in to the named collection.
     *
     * @param collection the collection to update
     * @param entity     the entity to save
     * @param options    the options to apply to the save operation
     * @param <T>        the type of the entity
     * @return the new key of the inserted entity
     */
    <T> Key<T> save(String collection, T entity, InsertOneOptions options);

}
