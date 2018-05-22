package org.mongodb.morphia.geo;

import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.GeoJson.geometryCollection;
import static org.mongodb.morphia.geo.GeoJson.lineString;
import static org.mongodb.morphia.geo.GeoJson.multiLineString;
import static org.mongodb.morphia.geo.GeoJson.multiPoint;
import static org.mongodb.morphia.geo.GeoJson.multiPolygon;
import static org.mongodb.morphia.geo.GeoJson.point;
import static org.mongodb.morphia.geo.GeoJson.polygon;
import static org.mongodb.morphia.geo.GeoJson.position;
import static org.mongodb.morphia.testutil.JSONMatcher.jsonEqual;

/**
 * Test driving features for Issue 643 - add support for saving entities with GeoJSON.
 */
@Ignore("Defer fixing the geo tests until after the core is fixed")
public class GeoEntitiesTest extends TestBase {
    @Test
    public void shouldConvertPointCorrectlyToDocument() {
        // given
        City city = new City("New City", point(3.0, 7.0));

        // when
        Document document = getMapper().toDocument(city);

        assertThat(document, is(notNullValue()));
        assertThat(document.toString(), jsonEqual("  {"
                                                  + " name: 'New City',"
                                                  + " className: 'org.mongodb.morphia.geo.City',"
                                                  + " location:  "
                                                  + " {"
                                                  + "  type: 'Point', "
                                                  + "  coordinates: [7.0, 3.0]"
                                                  + " }"
                                                  + "}"));
    }

    @Test
    public void shouldRetrieveGeoCollectionType() {
        // given
        String name = "What, everything?";
        Point point = point(3.0, 7.0);
        LineString lineString = lineString(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13));
        Polygon polygonWithHoles = polygon(
            lineString(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            lineString(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)),
            lineString(
                position(2.2, 2.1),
                position(2.4, 1.9),
                position(2.4, 1.7),
                position(2.1, 1.8),
                position(2.2, 2.1)));
        MultiPoint multiPoint = multiPoint(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13));
        MultiLineString multiLineString = multiLineString(asList(
            asList(
                position(1.0, 2.0),
                position(3, 5.0),
                position(19, 13)),
            asList(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0))));
        MultiPolygon multiPolygon = multiPolygon(
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            polygon(lineString(
                position(1.2, 3.0),
                position(2.5, 4.5),
                position(6.7, 1.9),
                position(1.2, 3.0)),
                lineString(
                    position(3.5, 2.4),
                    position(1.7, 2.8),
                    position(3.5, 2.4))));

        GeometryCollection geometryCollection = geometryCollection(point, lineString, polygonWithHoles, multiPoint,
            multiLineString, multiPolygon);

        AllTheThings allTheThings = new AllTheThings(name, geometryCollection);
        getDatastore().save(allTheThings);

        // when
        AllTheThings found = getDatastore().find(AllTheThings.class).field("name").equal(name).get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(allTheThings));
    }

    @Test
    public void shouldRetrieveGeoJsonLineString() {
        // given
        Route route = new Route("My Route", lineString(position(1.0, 2.0), position(3, 5.0),
            position(19, 13)));
        getDatastore().save(route);

        // when
        Route found = getDatastore().find(Route.class).field("name").equal("My Route").get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(route));
    }

    @Test
    public void shouldRetrieveGeoJsonMultiLineString() {
        // given
        String name = "Many Paths";
        Paths paths = new Paths(name, multiLineString(asList(
            asList(
                position(1.0, 2.0),
                position(3, 5.0),
                position(19, 13)),
            asList(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)))));
        getDatastore().save(paths);

        // when
        Paths found = getDatastore().find(Paths.class).field("name").equal(name).get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(paths));
    }

    @Test
    public void shouldRetrieveGeoJsonMultiPoint() {
        // given
        String name = "My stores";
        Stores stores = new Stores(name, multiPoint(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13)));
        getDatastore().save(stores);

        // when
        Stores found = getDatastore().find(Stores.class).field("name").equal(name).get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(stores));
    }

    @Test
    public void shouldRetrieveGeoJsonMultiPolygon() {
        // given
        String name = "All these shapes";
        Polygon polygonWithHoles = polygon(
            lineString(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            lineString(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)),
            lineString(
                position(2.2, 2.1),
                position(2.4, 1.9),
                position(2.4, 1.7),
                position(2.1, 1.8),
                position(2.2, 2.1)));
        Regions regions = new Regions(name, multiPolygon(
            polygon(
                point(1.1, 2.0),
                point(2.3, 3.5),
                point(3.7, 1.0),
                point(1.1, 2.0)),
            polygonWithHoles));
        getDatastore().save(regions);

        // when
        Regions found = getDatastore().find(Regions.class).field("name").equal(name).get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(regions));
    }

    @Test
    public void shouldRetrieveGeoJsonMultiRingPolygon() {
        // given
        String polygonName = "A polygon with holes";
        Polygon polygonWithHoles = polygon(
            lineString(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            lineString(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)),
            lineString(
                position(2.2, 2.1),
                position(2.4, 1.9),
                position(2.4, 1.7),
                position(2.1, 1.8),
                position(2.2, 2.1)));
        Area area = new Area(polygonName, polygonWithHoles);
        getDatastore().save(area);

        // when
        Area found = getDatastore().find(Area.class).field("name").equal(polygonName).get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(area));
    }

    @Test
    public void shouldRetrieveGeoJsonPoint() {
        // given
        City city = new City("New City", point(3.0, 7.0));
        getDatastore().save(city);

        // when
        City found = getDatastore().find(City.class).field("name").equal("New City").get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(city));
    }

    @Test
    public void shouldRetrieveGeoJsonPolygon() {
        // given
        Area area = new Area("The Area",
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)));
        getDatastore().save(area);

        // when
        Area found = getDatastore().find(Area.class).field("name").equal("The Area").get();

        // then
        assertThat(found, is(notNullValue()));
        assertThat(found, is(area));
    }

    @Test
    public void shouldSaveAnEntityWithAGeoCollectionType() {
        // given
        String name = "What, everything?";
        Point point = point(3.0, 7.0);
        LineString lineString = lineString(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13));
        Polygon polygonWithHoles = polygon(
            lineString(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            lineString(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)),
            lineString(
                position(2.2, 2.1),
                position(2.4, 1.9),
                position(2.4, 1.7),
                position(2.1, 1.8),
                position(2.2, 2.1)));
        MultiPoint multiPoint = multiPoint(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13));
        MultiLineString multiLineString = multiLineString(asList(
            asList(
                position(1.0, 2.0),
                position(3, 5.0),
                position(19, 13)),
            asList(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0))));
        MultiPolygon multiPolygon = multiPolygon(
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            polygon(lineString(
                position(1.2, 3.0),
                position(2.5, 4.5),
                position(6.7, 1.9),
                position(1.2, 3.0)),
                lineString(
                    position(3.5, 2.4),
                    position(1.7, 2.8),
                    position(3.5, 2.4))));

        GeometryCollection geometryCollection = geometryCollection(point, lineString, polygonWithHoles,
            multiPoint, multiLineString, multiPolygon);
        AllTheThings allTheThings = new AllTheThings(name, geometryCollection);

        // when
        getDatastore().save(allTheThings);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedArea = getDatabase().getCollection("allthethings")
                                           .find(new Document("name", name))
                                           .projection(new Document("_id", 0)
                                                           .append("className", 0))
                                           .iterator()
                                           .tryNext();
        assertThat(storedArea, is(notNullValue()));
        assertThat(storedArea.toString(), jsonEqual("  {"
                                                    + " name: '" + name + "',"
                                                    + " everything: "
                                                    + " {"
                                                    + "  type: 'GeometryCollection', "
                                                    + "  geometries: "
                                                    + "  ["
                                                    + "    {"
                                                    + "     type: 'Point', "
                                                    + "     coordinates: [7.0, 3.0]"
                                                    + "    }, "
                                                    + "    {"
                                                    + "     type: 'LineString', "
                                                    + "     coordinates: [ [ 2.0,  1.0],"
                                                    + "                    [ 5.0,  3.0],"
                                                    + "                    [13.0, 19.0] ]"
                                                    + "    },"
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
                                                    + "    {"
                                                    + "     type: 'MultiPoint', "
                                                    + "     coordinates: [ [ 2.0,  1.0],"
                                                    + "                    [ 5.0,  3.0],"
                                                    + "                    [13.0, 19.0] ]"
                                                    + "    },"
                                                    + "    {"
                                                    + "     type: 'MultiLineString', "
                                                    + "     coordinates: "
                                                    + "        [ [ [ 2.0,  1.0],"
                                                    + "            [ 5.0,  3.0],"
                                                    + "            [13.0, 19.0] "
                                                    + "          ], "
                                                    + "          [ [ 2.0, 1.5],"
                                                    + "            [ 2.0, 1.9],"
                                                    + "            [ 1.8, 1.9],"
                                                    + "            [ 2.0, 1.5] "
                                                    + "          ]"
                                                    + "        ]"
                                                    + "    },"
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
    public void shouldSaveAnEntityWithALineStringGeoJsonType() {
        // given
        Route route = new Route("My Route", lineString(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13)));

        // when
        getDatastore().save(route);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedRoute = getDatabase().getCollection("route")
                                            .find(new Document("name", "My Route"))
                                            .projection(new Document("_id", 0)
                                                            .append("className", 0))
                                            .iterator()
                                            .tryNext();
        assertThat(storedRoute, is(notNullValue()));
        // lat/long is always long/lat on the server
        assertThat(storedRoute.toString(), jsonEqual("  {"
                                                     + " name: 'My Route',"
                                                     + " route:"
                                                     + " {"
                                                     + "  type: 'LineString', "
                                                     + "  coordinates: [ [ 2.0,  1.0],"
                                                     + "                 [ 5.0,  3.0],"
                                                     + "                 [13.0, 19.0] ]"
                                                     + " }"
                                                     + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithALocationStoredAsAMultiPoint() {
        // given
        String name = "My stores";
        Stores stores = new Stores(name, multiPoint(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13)));

        // when
        getDatastore().save(stores);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedObject = getDatabase().getCollection("stores")
                                             .find(new Document("name", name))
                                             .projection(new Document("_id", 0)
                                                             .append("className", 0))
                                             .iterator()
                                             .tryNext();

        assertThat(storedObject, is(notNullValue()));
        assertThat(storedObject.toString(), jsonEqual("  {"
                                                      + " name: " + name + ","
                                                      + " locations:  "
                                                      + " {"
                                                      + "  type: 'MultiPoint', "
                                                      + "  coordinates: [ [ 2.0,  1.0],"
                                                      + "                 [ 5.0,  3.0],"
                                                      + "                 [13.0, 19.0] ]"
                                                      + " }"
                                                      + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithALocationStoredAsAPoint() {
        // given
        City city = new City("New City", point(3.0, 7.0));

        // when
        getDatastore().save(city);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedCity = getDatabase().getCollection("city")
                                           .find(new Document("name", "New City"))
                                           .projection(new Document("_id", 0)
                                                           .append("className", 0))
                                           .iterator()
                                           .tryNext();

        assertThat(storedCity, is(notNullValue()));
        assertThat(storedCity.toString(), jsonEqual("  {"
                                                    + " name: 'New City',"
                                                    + " location:  "
                                                    + " {"
                                                    + "  type: 'Point', "
                                                    + "  coordinates: [7.0, 3.0]"
                                                    + " }"
                                                    + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithAMultiLineStringGeoJsonType() {
        // given
        String name = "Many Paths";
        Paths paths = new Paths(name, multiLineString(asList(
            asList(
                position(1.0, 2.0),
                position(3, 5.0),
                position(19, 13)),
            asList(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)))));

        // when
        getDatastore().save(paths);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedPaths = getDatabase().getCollection("paths")
                                            .find(new Document("name", name))
                                            .projection(new Document("_id", 0)
                                                            .append("className", 0))
                                            .iterator()
                                            .tryNext();

        assertThat(storedPaths, is(notNullValue()));
        // lat/long is always long/lat on the server
        assertThat(storedPaths.toString(), jsonEqual("  {"
                                                     + " name: '" + name + "',"
                                                     + " paths:"
                                                     + " {"
                                                     + "  type: 'MultiLineString', "
                                                     + "  coordinates: "
                                                     + "     [ [ [ 2.0,  1.0],"
                                                     + "         [ 5.0,  3.0],"
                                                     + "         [13.0, 19.0] "
                                                     + "       ], "
                                                     + "       [ [ 2.0, 1.5],"
                                                     + "         [ 2.0, 1.9],"
                                                     + "         [ 1.8, 1.9],"
                                                     + "         [ 2.0, 1.5] "
                                                     + "       ]"
                                                     + "     ]"
                                                     + " }"
                                                     + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithAMultiPolygonGeoJsonType() {
        // given
        String name = "All these shapes";
        Polygon polygonWithHoles = polygon(
            lineString(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            lineString(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)),
            lineString(
                position(2.2, 2.1),
                position(2.4, 1.9),
                position(2.4, 1.7),
                position(2.1, 1.8),
                position(2.2, 2.1)));
        final MultiPolygon multiPolygon = multiPolygon(
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            polygonWithHoles);
        Regions regions = new Regions(name, multiPolygon);

        // when
        getDatastore().save(regions);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedRegions = getDatabase().getCollection("regions")
                                              .find(new Document("name", name))
                                              .projection(new Document("_id", 0)
                                                              .append("className", 0))
                                              .iterator()
                                              .tryNext();
        assertThat(storedRegions, is(notNullValue()));
        assertThat(storedRegions.toString(), jsonEqual("  {"
                                                       + " name: '" + name + "',"
                                                       + " regions:  "
                                                       + " {"
                                                       + "  type: 'MultiPolygon', "
                                                       + "  coordinates: [ [ [ [ 2.0, 1.1],"
                                                       + "                     [ 3.5, 2.3],"
                                                       + "                     [ 1.0, 3.7],"
                                                       + "                     [ 2.0, 1.1],"
                                                       + "                   ]"
                                                       + "                 ],"
                                                       + "                 [ [ [ 2.0, 1.1],"
                                                       + "                     [ 3.5, 2.3],"
                                                       + "                     [ 1.0, 3.7],"
                                                       + "                     [ 2.0, 1.1] "
                                                       + "                   ],"
                                                       + "                   [ [ 2.0, 1.5],"
                                                       + "                     [ 2.0, 1.9],"
                                                       + "                     [ 1.8, 1.9],"
                                                       + "                     [ 2.0, 1.5] "
                                                       + "                   ],"
                                                       + "                   [ [ 2.1, 2.2],"
                                                       + "                     [ 1.9, 2.4],"
                                                       + "                     [ 1.7, 2.4],"
                                                       + "                     [ 1.8, 2.1],"
                                                       + "                     [ 2.1, 2.2] "
                                                       + "                   ]"
                                                       + "                 ]"
                                                       + "               ]"
                                                       + " }"
                                                       + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithAPolygonContainingInteriorRings() {
        // given
        String polygonName = "A polygon with holes";
        Polygon polygonWithHoles = polygon(
            lineString(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)),
            lineString(
                position(1.5, 2.0),
                position(1.9, 2.0),
                position(1.9, 1.8),
                position(1.5, 2.0)),
            lineString(
                position(2.2, 2.1),
                position(2.4, 1.9),
                position(2.4, 1.7),
                position(2.1, 1.8),
                position(2.2, 2.1)));
        Area area = new Area(polygonName, polygonWithHoles);

        // when
        getDatastore().save(area);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedArea = getDatabase().getCollection("area")
                                           .find(new Document("name", polygonName))
                                           .projection(new Document("_id", 0)
                                                           .append("className", 0)
                                                           .append("area.className", 0))
                                           .iterator()
                                           .tryNext();
        assertThat(storedArea, is(notNullValue()));
        assertThat(storedArea.toString(), jsonEqual("  {"
                                                    + " name: " + polygonName + ","
                                                    + " area:  "
                                                    + " {"
                                                    + "  type: 'Polygon', "
                                                    + "  coordinates: "
                                                    + "    [ [ [ 2.0, 1.1],"
                                                    + "        [ 3.5, 2.3],"
                                                    + "        [ 1.0, 3.7],"
                                                    + "        [ 2.0, 1.1] "
                                                    + "      ],"
                                                    + "      [ [ 2.0, 1.5],"
                                                    + "        [ 2.0, 1.9],"
                                                    + "        [ 1.8, 1.9],"
                                                    + "        [ 2.0, 1.5] "
                                                    + "      ],"
                                                    + "      [ [ 2.1, 2.2],"
                                                    + "        [ 1.9, 2.4],"
                                                    + "        [ 1.7, 2.4],"
                                                    + "        [ 1.8, 2.1],"
                                                    + "        [ 2.1, 2.2] "
                                                    + "      ]"
                                                    + "    ]"
                                                    + " }"
                                                    + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithAPolygonGeoJsonType() {
        // given
        Area area = new Area("The Area",
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)));

        // when
        getDatastore().save(area);

        // then use the underlying driver to ensure it was persisted correctly to the database
        Document storedArea = getDatabase().getCollection("area")
                                           .find(new Document("name", "The Area"))
                                           .projection(new Document("_id", 0)
                                                           .append("className", 0)
                                                           .append("area.className", 0))
                                           .iterator()
                                           .tryNext();
        assertThat(storedArea, is(notNullValue()));
        assertThat(storedArea.toString(), jsonEqual("  {"
                                                    + " name: 'The Area',"
                                                    + " area:  "
                                                    + " {"
                                                    + "  type: 'Polygon', "
                                                    + "  coordinates: [ [ [ 2.0, 1.1],"
                                                    + "                   [ 3.5, 2.3],"
                                                    + "                   [ 1.0, 3.7],"
                                                    + "                   [ 2.0, 1.1] ] ]"
                                                    + " }"
                                                    + "}"));
    }

    @Test
    public void shouldSaveAnEntityWithNullPoints() {
        getDatastore().save(new City("New City", null));

        Document storedCity = getDatabase().getCollection("city")
                                           .find(new Document("name", "New City"))
                                           .projection(new Document("_id", 0)
                                                           .append("className", 0))
                                           .iterator()
                                           .tryNext();
        assertThat(storedCity, is(notNullValue()));
        assertThat(storedCity.toString(), jsonEqual("{ name: 'New City'}"));
    }


    @SuppressWarnings("UnusedDeclaration")
    private static final class Paths {
        @Id
        private String name;
        private MultiLineString paths;

        private Paths() {
        }

        private Paths(final String name, final MultiLineString paths) {
            this.name = name;
            this.paths = paths;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + paths.hashCode();
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Paths paths1 = (Paths) o;

            if (!name.equals(paths1.name)) {
                return false;
            }
            if (!paths.equals(paths1.paths)) {
                return false;
            }

            return true;
        }


        @Override
        public String toString() {
            return "Paths{"
                   + "name='" + name + '\''
                   + ", paths=" + paths
                   + '}';
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    private static final class AllTheThings {
        private GeometryCollection everything;
        private String name;

        private AllTheThings() {
        }

        private AllTheThings(final String name, final GeometryCollection everything) {
            this.name = name;
            this.everything = everything;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            AllTheThings that = (AllTheThings) o;

            if (!everything.equals(that.everything)) {
                return false;
            }
            if (!name.equals(that.name)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = everything.hashCode();
            result = 31 * result + name.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "AllTheThings{"
                   + "everything=" + everything
                   + ", name='" + name + '\''
                   + '}';
        }
    }
}
