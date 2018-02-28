package org.mongodb.morphia.query;

import org.bson.Document;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.testutil.JSONMatcher;

import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.PointBuilder.pointBuilder;

public class GeoFieldCriteriaTest extends TestBase {
    @Test
    public void shouldCreateCorrectNearQueryWithMaxDistance() {
        // given
        int maxDistanceMeters = 13;
        double latitude = 3.2;
        double longitude = 5.7;
        QueryImpl<Object> stubQuery = (QueryImpl<Object>) getDatastore().find(Object.class);
        stubQuery.disableValidation();
        GeoNearFieldCriteria criteria = new GeoNearFieldCriteria(stubQuery, "location", pointBuilder()
                                                                                            .latitude(latitude)
                                                                                            .longitude(longitude)
                                                                                            .build(),
            maxDistanceMeters);

        // when
        Document queryDocument = new Document();
        criteria.addTo(queryDocument);

        // then
        assertThat(queryDocument.toString(), JSONMatcher.jsonEqual("  { location : "
                                                                   + "  { $near : "
                                                                   + "    { $geometry : "
                                                                   + "      { type : 'Point' , "
                                                                   + "        coordinates : [ " + longitude + " , " + latitude + "]"
                                                                   + "      }, "
                                                                   + "      $maxDistance : " + maxDistanceMeters
                                                                   + "    }"
                                                                   + "  }"
                                                                   + "}"));
    }

    @Test
    public void shouldCreateCorrectNearQueryWithoutMaxDistance() {
        // given
        double latitude = 3.2;
        double longitude = 5.7;
        QueryImpl<Object> stubQuery = (QueryImpl<Object>) getDatastore().find(Object.class);
        stubQuery.disableValidation();

        GeoNearFieldCriteria criteria = new GeoNearFieldCriteria(stubQuery, "location", pointBuilder()
                                                                                            .latitude(latitude)
                                                                                            .longitude(longitude)
                                                                                            .build());

        // when
        Document queryDocument = new Document();
        criteria.addTo(queryDocument);


        // then
        assertThat(queryDocument.toString(), JSONMatcher.jsonEqual("  { location : "
                                                                   + "  { $near : "
                                                                   + "    { $geometry : "
                                                                   + "      { type : 'Point' , "
                                                                   + "        coordinates : [ " + longitude + " , " + latitude + "]"
                                                                   + "      } "
                                                                   + "    }"
                                                                   + "  }"
                                                                   + "}"));
    }
}
