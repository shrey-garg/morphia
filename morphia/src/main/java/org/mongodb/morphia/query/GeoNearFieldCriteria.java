package org.mongodb.morphia.query;

import com.mongodb.client.model.geojson.Geometry;
import org.bson.Document;
import org.mongodb.morphia.geo.CoordinateReferenceSystem;

import static org.mongodb.morphia.query.FilterOperator.NEAR;

/**
 * Creates queries for GeoJson geo queries on MongoDB. These queries generally require MongoDB 2.4 and above, and usually work on 2d sphere
 * indexes.
 */
class GeoNearFieldCriteria extends FieldCriteria {
    private final Integer maxDistanceMeters;
    private final Geometry  geometry;

    GeoNearFieldCriteria(final QueryImpl<?> query, final String field, final Geometry value) {
        super(query, field, NEAR, value);
        this.maxDistanceMeters = null;
        geometry = value;
    }

    GeoNearFieldCriteria(final QueryImpl<?> query, final String field, final Geometry value, final Integer maxDistanceMeters) {
        super(query, field, NEAR, value);
        this.maxDistanceMeters = maxDistanceMeters;
        geometry = value;
    }

    @Override
    public void addTo(final Document obj) {

        if (maxDistanceMeters != null) {
            // TODO: reinstate this
//            geometry.put("$maxDistance", maxDistanceMeters);
        }
        Document query = new Document(getOperator().val(), geometry);

        obj.put(getField(), query);
    }
}
