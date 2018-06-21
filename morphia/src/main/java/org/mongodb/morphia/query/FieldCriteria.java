package org.mongodb.morphia.query;


import org.bson.Document;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.PropertyHandler;

import java.util.Map;

import static org.mongodb.morphia.query.QueryValidator.validateQuery;

/**
 * Defines a Criteria against a field
 */
class FieldCriteria extends AbstractCriteria {
    private final String field;
    private final FilterOperator operator;
    private final Object value;
    private final boolean not;

    FieldCriteria(final QueryImpl<?> query, final String field, final FilterOperator op, final Object value) {
        this(query, field, op, value, false);
    }

    @SuppressWarnings("deprecation")
    FieldCriteria(final QueryImpl<?> query, final String fieldName, final FilterOperator op, final Object value, final boolean not) {
        //validate might modify prop string to translate java field name to db field name
        final StringBuilder sb = new StringBuilder(fieldName);
        final Mapper mapper = query.getDatastore().getMapper();

        final MappedField mappedField = validateQuery(query.getEntityClass(), mapper, sb, op, value, query.isValidatingNames(),
            query.isValidatingTypes());

        Object mappedValue = value;
        if (mappedField != null) {
            PropertyHandler handler = mappedField.getHandler();
            if(handler != null) {
                mappedValue = handler.encodeValue(value);
            }
        }

        this.field = sb.toString();
        this.operator = op;
        this.value = mappedValue;
        this.not = not;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addTo(final Document obj) {
        if (FilterOperator.EQUAL.equals(operator)) {
            // no operator, prop equals (or NOT equals) value
            if (not) {
                obj.put(field, new Document("$not", value));
            } else {
                obj.put(field, value);
            }

        } else {
            final Object object = obj.get(field); // operator within inner object
            Document inner;
            if (!(object instanceof Map)) {
                inner = new Document();
                obj.put(field, inner);
            } else {
                inner = (Document) object;
            }

            if (not) {
                inner.put("$not", new Document(operator.val(), value));
            } else {
                inner.put(operator.val(), value);
            }
        }
    }

    @Override
    public String getFieldName() {
        return field;
    }

    /**
     * @return the field
     */
    public String getField() {
        return field;
    }

    /**
     * @return the operator used against this field
     * @see FilterOperator
     */
    public FilterOperator getOperator() {
        return operator;
    }

    /**
     * @return the value used in the Criteria
     */
    public Object getValue() {
        return value;
    }

    /**
     * @return true if 'not' has been applied against this Criteria
     */
    public boolean isNot() {
        return not;
    }

    @Override
    public String toString() {
        return field + " " + operator.val() + " " + value;
    }
}
