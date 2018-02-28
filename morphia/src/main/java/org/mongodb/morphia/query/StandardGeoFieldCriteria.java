package org.mongodb.morphia.query;

import org.bson.Document;
import org.mongodb.morphia.geo.CoordinateReferenceSystem;
import org.mongodb.morphia.geo.Geometry;

import static org.mongodb.morphia.query.FilterOperator.NEAR;

/**
 * Creates queries for GeoJson geo queries on MongoDB. These queries generally require MongoDB 2.4 and above, and usually work on 2d sphere
 * indexes.
 */
class StandardGeoFieldCriteria extends FieldCriteria {
    private final Integer maxDistanceMeters;
    private final Geometry geometry;
    private CoordinateReferenceSystem crs;

    StandardGeoFieldCriteria(final QueryImpl<?> query, final String field, final FilterOperator operator, final Geometry value,
                             final Integer maxDistanceMeters, final CoordinateReferenceSystem crs) {
        this(query, field, operator, value, maxDistanceMeters);
        this.crs = crs;
    }

    @SuppressWarnings("deprecation")
    StandardGeoFieldCriteria(final QueryImpl<?> query, final String field, final FilterOperator operator, final Geometry value,
                             final Integer maxDistanceMeters) {
        super(query, field, operator, value);
        this.maxDistanceMeters = maxDistanceMeters;
        geometry = value;
    }

    @Override
    public void addTo(final Document obj) {
        Document query;
        FilterOperator operator = getOperator();

        switch (operator) {
            case NEAR:
                if (maxDistanceMeters != null) {
                    geometry.put("$maxDistance", maxDistanceMeters);
                }
                query = new Document(NEAR.val(), geometry);
                break;
            case GEO_WITHIN:
            case INTERSECTS:
                query = new Document(operator.val(), geometry);
                if (crs != null) {
                    ((Document) geometry.get("$geometry")).put("crs", crs);
                }
                break;
            default:
                throw new UnsupportedOperationException(String.format("Operator %s not supported for geo-query", operator.val()));
        }

        obj.put(getField(), query);
    }
}
