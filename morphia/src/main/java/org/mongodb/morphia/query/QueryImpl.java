package org.mongodb.morphia.query;


import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOptions;
import org.bson.Document;
import org.bson.types.CodeWScope;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.DatastoreImpl;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.logging.Logger;
import org.mongodb.morphia.logging.MorphiaLoggerFactory;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mongodb.morphia.query.QueryValidator.validateQuery;


/**
 * Implementation of Query
 *
 * @param <T> The type we will be querying for, and returning.
 */
public class QueryImpl<T> extends CriteriaContainerImpl implements Query<T> {
    private static final Logger LOG = MorphiaLoggerFactory.get(QueryImpl.class);
    private final DatastoreImpl ds;
    private final MongoCollection<T> collection;
    private final Class<T> clazz;
    private boolean validateName = true;
    private boolean validateType = true;
    private Boolean includeFields;
    private Document baseQuery;
    private Document projection = new Document();
    private Document sort = new Document();
    private ReadConcern readConcern;
    private ReadPreference readPreference;

    /**
     * Creates a Query for the given type and collection
     *
     * @param clazz the type to return
     * @param collection  the collection to query
     * @param ds    the Datastore to use
     */
    public QueryImpl(final Class<T> clazz, final MongoCollection<T> collection, final Datastore ds) {
        super(CriteriaJoin.AND);

        setQuery(this);
        this.clazz = clazz;
        this.ds = (DatastoreImpl) ds;
        this.collection = collection;
        readPreference = getDatastore().getDatabase().getReadPreference();
        readConcern = getDatastore().getDatabase().getReadConcern();
    }

    @Override
    public Query<T> setReadConcern(final ReadConcern readConcern) {
        this.readConcern = readConcern;
        return this;
    }

    @Override
    public Query<T> setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
        return this;
    }

    /**
     * Parses the string and validates each part
     *
     * @param str      the String to parse
     * @param clazz    the class to use when validating
     * @param mapper   the Mapper to use
     * @param validate true if the results should be validated
     * @return the DBObject
     */
    private static Document parseFieldsString(final String str, final Class clazz, final Mapper mapper, final boolean validate) {
        Document ret = new Document();
        final String[] parts = str.split(",");
        for (String s : parts) {
            s = s.trim();
            int dir = 1;

            if (s.startsWith("-")) {
                dir = -1;
                s = s.substring(1).trim();
            }

            if (validate) {
                final StringBuilder sb = new StringBuilder(s);
                validateQuery(clazz, mapper, sb, FilterOperator.IN, "", true, false);
                s = sb.toString();
            }
            ret.put(s, dir);
        }
        return ret;
    }

    @Override
    public List<Key<T>> asKeyList() {
        return asKeyList(new FindOptions());
    }

    @Override
    public List<Key<T>> asKeyList(final FindOptions options) {
        final List<Key<T>> results = new ArrayList<>();
        try (MorphiaKeyIterator<T> keys = fetchKeys(options)) {
            while (keys.hasNext()) {
                results.add(keys.next());
            }
        }
        return results;
    }

    @Override
    public List<T> asList() {
        return asList(new FindOptions());
    }

    @Override
    public List<T> asList(final FindOptions options) {
        final List<T> results = new ArrayList<>();
        try (MongoCursor<T> entities = fetch(options)) {
            while (entities.hasNext()) {
                results.add(entities.next());
            }
        }

        return results;
    }

    @Override
    @Deprecated
    public long countAll() {
        final Document query = getQueryDocument();
        if (LOG.isTraceEnabled()) {
            LOG.trace("Executing count(" + collection.getNamespace().getCollectionName() + ") for query: " + query);
        }
        return collection.count(query);
    }

    @Override
    public long count() {
        return collection.count(getQueryDocument());
    }

    @Override
    public long count(final CountOptions options) {
        return collection.count(getQueryDocument(), options);
    }

    @Override
    public MongoCursor<T> fetch() {
        return fetch(new FindOptions());
    }

    @Override
    public MongoCursor<T> fetch(final FindOptions options) {
        final FindIterable<T> cursor = prepareCursor(options);
        if (LOG.isTraceEnabled()) {
            LOG.trace("Getting cursor(" + collection.getNamespace().getCollectionName() + ")  for query:" + getQuery());
        }

        return cursor.iterator();
    }

    @Override
    public MongoCursor<T> fetchEmptyEntities() {
        return fetchEmptyEntities(new FindOptions());
    }

    @Override
    public MongoCursor<T> fetchEmptyEntities(final FindOptions options) {
        QueryImpl<T> cloned = cloneQuery();
        cloned.includeFields = true;
        return cloned.fetch(new FindOptions(options)
                                .projection(new Document(Mapper.ID_KEY, 1)));
    }

    @Override
    public MorphiaKeyIterator<T> fetchKeys() {
        return fetchKeys(new FindOptions());
    }

    @Override
    public MorphiaKeyIterator<T> fetchKeys(final FindOptions options) {

        final FindOptions projection = new FindOptions(options)
                                           .projection(new Document(Mapper.ID_KEY, 1));
        return new MorphiaKeyIterator<>(
            prepareCursor(projection).iterator(), ds.getMapper());
    }

    @Override
    public T get() {
        return get(new FindOptions());
    }

    @Override
    public T get(final FindOptions options) {
        try (MongoCursor<T> it = fetch(new FindOptions(options)
                                           .limit(1))) {
            return (it.hasNext()) ? it.next() : null;
        }
    }

    @Override
    public Key<T> getKey() {
        return getKey(new FindOptions());
    }

    @Override
    public Key<T> getKey(final FindOptions options) {
        final MorphiaKeyIterator<T> it = fetchKeys(new FindOptions(options)
                                                            .limit(1));
        Key<T> key = (it.hasNext()) ? it.next() : null;
        it.close();
        return key;
    }

    @Override
    public QueryImpl<T> cloneQuery() {
        final QueryImpl<T> n = new QueryImpl<>(clazz, collection, ds);
        n.includeFields = includeFields;
        n.setQuery(n); // feels weird, correct?
        n.validateName = validateName;
        n.validateType = validateType;
        n.baseQuery = copy(baseQuery);

        // fields from superclass
        n.setAttachedTo(getAttachedTo());
        n.setChildren(getChildren() == null ? null : new ArrayList<>(getChildren()));
        return n;
    }

    protected Document copy(final Document document) {
        return document == null ? null : new Document(document);
    }

    @Override
    public FieldEnd<? extends CriteriaContainerImpl> criteria(final String field) {
        final CriteriaContainerImpl container = new CriteriaContainerImpl(this, CriteriaJoin.AND);
        add(container);

        return new FieldEndImpl<>(this, field, container);
    }

    @Override
    public Query<T> disableValidation() {
        validateName = false;
        validateType = false;
        return this;
    }

    @Override
    public Query<T> enableValidation() {
        validateName = true;
        validateType = true;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> explain() {
        return explain(new FindOptions());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> explain(final FindOptions options) {
        return new LinkedHashMap<>(getDatastore().getDatabase()
                                                 .runCommand(getQueryDocument()));
    }

    @Override
    public FieldEnd<? extends Query<T>> field(final String name) {
        return new FieldEndImpl<>(this, name, this);
    }

    @Override
    public Query<T> filter(final String condition, final Object value) {
        final String[] parts = condition.trim().split(" ");
        if (parts.length < 1 || parts.length > 6) {
            throw new IllegalArgumentException("'" + condition + "' is not a legal filter condition");
        }

        final String prop = parts[0].trim();
        final FilterOperator op = (parts.length == 2) ? translate(parts[1]) : FilterOperator.EQUAL;

        add(new FieldCriteria(this, prop, op, value));

        return this;
    }

    @Override
    public MongoCollection<T> getCollection() {
        return collection;
    }

    @Override
    public Class<T> getEntityClass() {
        return clazz;
    }

    @Override
    public Document getFields() {
        if (projection == null || projection.keySet().size() == 0) {
            return null;
        }

        final MappedClass mc = ds.getMapper().getMappedClass(clazz);

        Entity entityAnnotation = mc.getEntityAnnotation();
        final Document fieldsFilter = new Document(projection);

        if (includeFields && entityAnnotation != null && !entityAnnotation.noClassnameStored()) {
            fieldsFilter.put(Mapper.CLASS_NAME_FIELDNAME, 1);
        }

        return fieldsFilter;
    }

    @Override
    public Document getQueryDocument() {
        final Document obj = new Document();

        if (baseQuery != null) {
            obj.putAll(baseQuery);
        }

        addTo(obj);

        return obj;
    }

    /**
     * Sets query structure directly
     *
     * @param query the Document containing the query
     */
    public void setQueryDocument(final Document query) {
        baseQuery = new Document(query);
    }

    @Override
    public Document getSortDocument() {
        return sort == null ? new Document() : new Document(sort);
    }

    @Override
    public Query<T> order(final String order) {
        sort.putAll(parseFieldsString(order, clazz, ds.getMapper(), validateName));
        return this;
    }

    @Override
    public Query<T> order(final Meta meta) {
        validateQuery(clazz, ds.getMapper(), new StringBuilder(meta.getField()), FilterOperator.IN, "", false, false);

        sort.putAll(meta.toDatabase());

        return this;
    }

    @Override
    public Query<T> order(final Sort... sorts) {
        Document sortList = new Document();
        for (Sort sort : sorts) {
            String s = sort.getField();
            if (validateName) {
                final StringBuilder sb = new StringBuilder(s);
                validateQuery(clazz, ds.getMapper(), sb, FilterOperator.IN, "", true, false);
                s = sb.toString();
            }
            sortList.put(s, sort.getOrder());
        }
        sort.putAll(sortList);
        return this;
    }

    @Override
    public Query<T> retrieveKnownFields() {
        for (final MappedField mf : ds.getMapper().getMappedClass(clazz).getPersistenceFields()) {
            project(mf.getNameToStore(), true);
        }
        return this;
    }

    @Override
    public Query<T> project(final String field, final boolean include) {
        final StringBuilder sb = new StringBuilder(field);
        validateQuery(clazz, ds.getMapper(), sb, FilterOperator.EQUAL, null, validateName, false);
        String fieldName = sb.toString();
        validateProjections(fieldName, include);
        project(fieldName, include ? 1 : 0);
        return this;
    }

    private void project(final String fieldName, final Object value) {
        projection.put(fieldName, value);
    }

    private void project(final Document value) {
        projection.putAll(value);
    }

    @Override
    public Query<T> project(final String field, final ArraySlice slice) {
        final StringBuilder sb = new StringBuilder(field);
        validateQuery(clazz, ds.getMapper(), sb, FilterOperator.EQUAL, null, validateName, false);
        String fieldName = sb.toString();
        validateProjections(fieldName, true);
        project(fieldName, slice.toDatabase());
        return this;
    }

    @Override
    public Query<T> project(final Meta meta) {
        final StringBuilder sb = new StringBuilder(meta.getField());
        validateQuery(clazz, ds.getMapper(), sb, FilterOperator.EQUAL, null, false, false);
        String fieldName = sb.toString();
        validateProjections(fieldName, true);
        project(meta.toDatabase());
        return this;

    }

    private void validateProjections(final String field, final boolean include) {
        if (includeFields != null && include != includeFields) {
            if (!includeFields || !"_id".equals(field)) {
                throw new ValidationException("You cannot mix included and excluded fields together");
            }
        }
        if (includeFields == null) {
            includeFields = include;
        }
    }

    @Override
    public Query<T> search(final String search) {
        final Document op = new Document("$search", search);
        this.criteria("$text").equal(op);
        return this;
    }

    @Override
    public Query<T> search(final String search, final String language) {
        final Document op = new Document("$search", search)
                                     .append("$language", language);
        this.criteria("$text").equal(op);
        return this;
    }

    @Override
    public Query<T> where(final String js) {
        add(new WhereCriteria(js));
        return this;
    }

    @Override
    public Query<T> where(final CodeWScope js) {
        add(new WhereCriteria(js));
        return this;
    }

    @Override
    public String getFieldName() {
        return null;
    }

    /**
     * @return the Datastore
     */
    public Datastore getDatastore() {
        return ds;
    }

    /**
     * @return true if field names are being validated
     */
    public boolean isValidatingNames() {
        return validateName;
    }

    /**
     * @return true if query parameter value types are being validated against the field types
     */
    public boolean isValidatingTypes() {
        return validateType;
    }

    @Override
    public MongoCursor<T> iterator() {
        return fetch();
    }

    private FindIterable<T> prepareCursor(final FindOptions findOptions) {
        final Document query = getQueryDocument();

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Running query(%s) : %s, options: %s,", collection.getNamespace().getCollectionName(), query, findOptions));
        }

        return collection
                   .withReadPreference(readPreference)
                   .withReadConcern(readConcern)
                   .find(query)
                         .projection(getFields())
                         .sort(getSortDocument());
    }

    @Override
    public String toString() {
        return String.format("{ query: %s %s }", getQueryDocument(), ", projection: " + getFields());
    }

    /**
     * Converts the textual operator (">", "<=", etc) into a FilterOperator. Forgiving about the syntax; != and <> are NOT_EQUAL, = and ==
     * are EQUAL.
     */
    private FilterOperator translate(final String operator) {
        return FilterOperator.fromString(operator);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final QueryImpl<?> query = (QueryImpl<?>) o;

        if (validateName != query.validateName) {
            return false;
        }
        if (validateType != query.validateType) {
            return false;
        }
        if (collection != null ? !collection.equals(query.collection) : query.collection != null) {
            return false;
        }
        if (clazz != null ? !clazz.equals(query.clazz) : query.clazz != null) {
            return false;
        }
        if (includeFields != null ? !includeFields.equals(query.includeFields) : query.includeFields != null) {
            return false;
        }
        if (baseQuery != null ? !baseQuery.equals(query.baseQuery) : query.baseQuery != null) {
            return false;
        }
        if (projection != null ? !projection.equals(query.projection) : query.projection != null) {
            return false;
        }
        if (sort != null ? !sort.equals(query.sort) : query.sort != null) {
            return false;
        }
        if (readConcern != null ? !readConcern.equals(query.readConcern) : query.readConcern != null) {
            return false;
        }
        return readPreference != null ? readPreference.equals(query.readPreference) : query.readPreference == null;
    }

    @Override
    public int hashCode() {
        int result = collection != null ? collection.hashCode() : 0;
        result = 31 * result + (clazz != null ? clazz.hashCode() : 0);
        result = 31 * result + (validateName ? 1 : 0);
        result = 31 * result + (validateType ? 1 : 0);
        result = 31 * result + (includeFields != null ? includeFields.hashCode() : 0);
        result = 31 * result + (baseQuery != null ? baseQuery.hashCode() : 0);
        result = 31 * result + (projection != null ? projection.hashCode() : 0);
        result = 31 * result + (sort != null ? sort.hashCode() : 0);
        result = 31 * result + (readConcern != null ? readConcern.hashCode() : 0);
        result = 31 * result + (readPreference != null ? readPreference.hashCode() : 0);
        return result;
    }
}
