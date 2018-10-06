package xyz.morphia.query;

import com.mongodb.client.model.geojson.CoordinateReferenceSystem;
import com.mongodb.client.model.geojson.Geometry;
import org.bson.Document;

/**
 * Creates queries for GeoJson geo queries on MongoDB. These queries generally require MongoDB 2.4 and above, and usually work on 2d sphere
 * indexes.
 */
class GeoIntersectsFieldCriteria extends FieldCriteria {
    private final Geometry  geometry;
    private CoordinateReferenceSystem crs;

    GeoIntersectsFieldCriteria(final QueryImpl<?> query, final String field, final Geometry value,
                               final CoordinateReferenceSystem crs, final FilterOperator operator) {
        this(query, field, value, operator);
        this.crs = crs;
    }

    GeoIntersectsFieldCriteria(final QueryImpl<?> query, final String field, final Geometry value, final FilterOperator operator) {
        super(query, field, operator, value);
        geometry = value;
    }

    @Override
    public void addTo(final Document obj) {
        Document query;
        FilterOperator operator = getOperator();

        switch (operator) {
            case GEO_WITHIN:
            case INTERSECTS:
                query = new Document(operator.val(), geometry);
                if (crs != null) {
                    // TODO: reinstate this
//                    ((Document) geometry.get("$geometry")).put("crs", crs);
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Operator %s not supported for geo-query", operator.val()));
        }

        obj.put(getField(), query);
    }
}
