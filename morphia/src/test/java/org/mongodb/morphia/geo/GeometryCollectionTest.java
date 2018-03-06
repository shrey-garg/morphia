package org.mongodb.morphia.geo;

import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Polygon;
import org.bson.Document;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.testutil.JSONMatcher;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.GeoJson.geometryCollection;
import static org.mongodb.morphia.geo.GeoJson.lineString;
import static org.mongodb.morphia.geo.GeoJson.multiPolygon;
import static org.mongodb.morphia.geo.GeoJson.point;
import static org.mongodb.morphia.geo.GeoJson.polygon;
import static org.mongodb.morphia.geo.GeoJson.position;

public class GeometryCollectionTest extends TestBase {
    @Test
    public void shouldCorrectlySerialiseLineStringsInGeometryCollection() {
        // given
        LineString lineString = lineString(
            position(1, 2),
            position(3, 5),
            position(19, 13));
        GeometryCollection geometryCollection = geometryCollection(lineString);

        // when
        Document document = getMorphia().getMapper().toDocument(geometryCollection);

        assertThat(document, is(notNullValue()));
        assertThat(document.toString(), JSONMatcher.jsonEqual("  {"
                                                              + "  type: 'GeometryCollection', "
                                                              + "  geometries: "
                                                              + "  ["
                                                              + "    {"
                                                              + "     type: 'LineString', "
                                                              + "     coordinates: [ [ 2.0,  1.0],"
                                                              + "                    [ 5.0,  3.0],"
                                                              + "                    [13.0, 19.0] ]"
                                                              + "    },"
                                                              + "  ]"
                                                              + "}"));
    }

    @Test
    public void shouldCorrectlySerialiseMultiPointsInGeometryCollection() {
        // given
        MultiPoint multiPoint = GeoJson.multiPoint(position(1, 2), position(3, 5),
            position(19, 13));
        GeometryCollection geometryCollection = geometryCollection(multiPoint);

        // when
        Document document = getMorphia().getMapper().toDocument(geometryCollection);

        assertThat(document, is(notNullValue()));
        assertThat(document.toString(), JSONMatcher.jsonEqual("  {"
                                                              + "  type: 'GeometryCollection', "
                                                              + "  geometries: "
                                                              + "  ["
                                                              + "    {"
                                                              + "     type: 'MultiPoint', "
                                                              + "     coordinates: [ [ 2.0,  1.0],"
                                                              + "                    [ 5.0,  3.0],"
                                                              + "                    [13.0, 19.0] ]"
                                                              + "    },"
                                                              + "  ]"
                                                              + " }"
                                                              + "}"));
    }

    @Test
    public void shouldCorrectlySerialiseMultiPolygonsInGeometryCollection() {
        // given
        MultiPolygon multiPolygon = multiPolygon(polygon(lineString(position(1.1, 2.0), position(2.3, 3.5),
            position(3.7, 1.0), position(1.1, 2.0))),
                                                 polygon(lineString(position(1.2, 3.0), position(2.5, 4.5),
                                                     position(6.7, 1.9), position(1.2, 3.0)),
                                                         lineString(position(3.5, 2.4), position(1.7, 2.8),
                                                             position(3.5, 2.4))));
        GeometryCollection geometryCollection = geometryCollection(multiPolygon);

        // when
        Document document = getMorphia().getMapper().toDocument(geometryCollection);

        assertThat(document, is(notNullValue()));
        assertThat(document.toString(), JSONMatcher.jsonEqual("  {"
                                                              + "  type: 'GeometryCollection', "
                                                              + "  geometries: "
                                                              + "  ["
                                                              + "    {"
                                                              + "     type: 'MultiPolygon', "
                                                              + "     coordinates: [ [ [ [ 2.0, 1.1],"
                                                              + "                        [ 3.5, 2.3],"
                                                              + "                        [ 1.0, 3.7],"
                                                              + "                        [ 2.0, 1.1],"
                                                              + "                      ]"
                                                              + "                    ],"
                                                              + "                    [ [ [ 3.0, 1.2],"
                                                              + "                        [ 4.5, 2.5],"
                                                              + "                        [ 1.9, 6.7],"
                                                              + "                        [ 3.0, 1.2] "
                                                              + "                      ],"
                                                              + "                      [ [ 2.4, 3.5],"
                                                              + "                        [ 2.8, 1.7],"
                                                              + "                        [ 2.4, 3.5] "
                                                              + "                      ],"
                                                              + "                    ]"
                                                              + "                  ]"
                                                              + "    }"
                                                              + "  ]"
                                                              + " }"
                                                              + "}"));
    }

    @Test
    public void shouldCorrectlySerialisePointsInGeometryCollection() {
        // given
        com.mongodb.client.model.geojson.Point point = point(3.0, 7.0);
        GeometryCollection geometryCollection = geometryCollection(point);

        // when
        Document document = getMorphia().getMapper().toDocument(geometryCollection);

        // then use the underlying driver to ensure it was persisted correctly to the database
        assertThat(document, is(notNullValue()));
        assertThat(document.toString(), JSONMatcher.jsonEqual("  {"
                                                              + "  type: 'GeometryCollection', "
                                                              + "  geometries: "
                                                              + "  ["
                                                              + "    {"
                                                              + "     type: 'Point', "
                                                              + "     coordinates: [7.0, 3.0]"
                                                              + "    }, "
                                                              + "  ]"
                                                              + "}"));
    }

    @Test
    public void shouldCorrectlySerialisePolygonsInGeometryCollection() {
        // given
        Polygon polygonWithHoles = polygon(lineString(position(1.1, 2.0), position(2.3, 3.5), position(3.7, 1.0),
            position(1.1, 2.0)),
                                           lineString(position(1.5, 2.0), position(1.9, 2.0), position(1.9, 1.8),
                                               position(1.5, 2.0)),
                                           lineString(position(2.2, 2.1), position(2.4, 1.9), position(2.4, 1.7),
                                               position(2.1, 1.8),
                                               position(2.2, 2.1)));
        GeometryCollection geometryCollection = geometryCollection(polygonWithHoles);

        // when
        Document document = getMorphia().getMapper().toDocument(geometryCollection);

        assertThat(document, is(notNullValue()));
        assertThat(document.toString(), JSONMatcher.jsonEqual("  {"
                                                              + "  type: 'GeometryCollection', "
                                                              + "  geometries: "
                                                              + "  ["
                                                              + "    {"
                                                              + "     type: 'Polygon', "
                                                              + "     coordinates: "
                                                              + "       [ [ [ 2.0, 1.1],"
                                                              + "           [ 3.5, 2.3],"
                                                              + "           [ 1.0, 3.7],"
                                                              + "           [ 2.0, 1.1] "
                                                              + "         ],"
                                                              + "         [ [ 2.0, 1.5],"
                                                              + "           [ 2.0, 1.9],"
                                                              + "           [ 1.8, 1.9],"
                                                              + "           [ 2.0, 1.5] "
                                                              + "         ],"
                                                              + "         [ [ 2.1, 2.2],"
                                                              + "           [ 1.9, 2.4],"
                                                              + "           [ 1.7, 2.4],"
                                                              + "           [ 1.8, 2.1],"
                                                              + "           [ 2.1, 2.2] "
                                                              + "         ]"
                                                              + "       ]"
                                                              + "    },"
                                                              + "  ]"
                                                              + " }"
                                                              + "}"));
    }
}
