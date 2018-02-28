package org.mongodb.morphia.query;


import org.bson.Document;

import java.util.Map;

/**
 * Geospatial specific FieldCriteria logic
 */
class GeoFieldCriteria extends FieldCriteria {

    private final Map<String, Object> opts;

    GeoFieldCriteria(final QueryImpl<?> query, final String field, final FilterOperator op, final Object value,
                     final Map<String, Object> opts) {
        super(query, field, op, value);
        this.opts = opts;
    }

    @Override
    public void addTo(final Document obj) {
        final Document query;
        switch (getOperator()) {
            case NEAR:
                query = new Document(FilterOperator.NEAR.val(), getValue());
                break;
            case NEAR_SPHERE:
                query = new Document(FilterOperator.NEAR_SPHERE.val(), getValue());
                break;
            case WITHIN_BOX:
                query = new Document(FilterOperator.GEO_WITHIN.val(), new Document(getOperator().val(), getValue()));
                break;
            case WITHIN_CIRCLE:
                query = new Document(FilterOperator.GEO_WITHIN.val(), new Document(getOperator().val(), getValue()));
                break;
            case WITHIN_CIRCLE_SPHERE:
                query = new Document(FilterOperator.GEO_WITHIN.val(), new Document(getOperator().val(), getValue()));
                break;
            default:
                throw new UnsupportedOperationException(getOperator() + " not supported for geo-query");
        }

        if (opts != null) {
            for (final Map.Entry<String, Object> e : opts.entrySet()) {
                query.append(e.getKey(), e.getValue());
            }
        }

        obj.put(getField(), query);
    }
}
