package org.mongodb.morphia.geo;

import com.mongodb.client.model.geojson.GeometryCollection;
import com.mongodb.client.model.geojson.LineString;
import com.mongodb.client.model.geojson.MultiLineString;
import com.mongodb.client.model.geojson.MultiPoint;
import com.mongodb.client.model.geojson.MultiPolygon;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Polygon;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Id;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
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
public class GeoEntitiesTest extends TestBase {

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
                    position(3.2, 1.4),
                    position(3.5, 2.4))));

        GeometryCollection geometryCollection = geometryCollection(point, lineString, polygonWithHoles, multiPoint,
            multiLineString, multiPolygon);

        AllTheThings allTheThings = new AllTheThings(name, geometryCollection);
        getDatastore().save(allTheThings);
        check(allTheThings.getId(), allTheThings);
    }

    @Test
    public void shouldRetrieveGeoJsonLineString() {
        // given
        Route route = new Route("My Route", lineString(position(1.0, 2.0), position(3, 5.0),
            position(19, 13)));
        getDatastore().save(route);
        check("My Route", route);
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
        check(name, paths);
    }

    @Test
    public void shouldRetrieveGeoJsonMultiPoint() {
        String name = "My stores";
        Stores stores = new Stores(name, multiPoint(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13)));
        getDatastore().save(stores);

        check(stores.getId(), stores);
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

        check(name, regions);
    }

    @Test
    public void shouldRetrieveGeoJsonMultiRingPolygon() {
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

        check(polygonName, area);
    }

    @Test
    public void shouldRetrieveGeoJsonPoint() {
        City city = new City("New City", point(3.0, 7.0));
        getDatastore().save(city);

        check("New City", city);
    }

    @Test
    public void shouldRetrieveGeoJsonPolygon() {
        Area area = new Area("The Area",
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)));
        getDatastore().save(area);

        check("The Area", area);
    }

    @Test
    public void shouldSaveAnEntityWithAGeoCollectionType() {
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
                    position(1.7, 2.8),
                    position(3.5, 2.4))));

        AllTheThings allTheThings = new AllTheThings(name, geometryCollection(point, lineString, polygonWithHoles,
            multiPoint, multiLineString, multiPolygon));

        getDatastore().save(allTheThings);
        check(allTheThings.getId(), allTheThings);
    }

    @Test
    public void shouldSaveAnEntityWithALineStringGeoJsonType() {
        Route route = new Route("My Route", lineString(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13)));

        getDatastore().save(route);
        check("My Route", route);
    }

    @Test
    public void shouldSaveAnEntityWithALocationStoredAsAMultiPoint() {
        String name = "My stores";
        Stores stores = new Stores(name, multiPoint(
            position(1.0, 2.0),
            position(3, 5.0),
            position(19, 13)));

        getDatastore().save(stores);
        check(stores.getId(), stores);
    }

    @Test
    public void shouldSaveAnEntityWithALocationStoredAsAPoint() {
        City city = new City("New City", point(3.0, 7.0));

        getDatastore().save(city);
        check("New City", city);
    }

    @Test
    public void shouldSaveAnEntityWithAMultiLineStringGeoJsonType() {
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
        check(name, paths);
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
        getDatastore().save(regions);
        check(name, regions);
    }

    @Test
    public void shouldSaveAnEntityWithAPolygonGeoJsonType() {
        final String name = "The Area";
        Area area = new Area(name,
            polygon(
                position(1.1, 2.0),
                position(2.3, 3.5),
                position(3.7, 1.0),
                position(1.1, 2.0)));

        getDatastore().save(area);
        check(name, area);
    }

    @Test
    public void shouldSaveAnEntityWithNullPoints() {
        final City city = new City("New City", null);
        getDatastore().save(city);
        check("New City", city);
    }

    private void check(final Object id, final Object original) {
        assertEquals(original, getDatastore().find(original.getClass())
                                   .filter("_id", id)
                                   .iterator()
                                   .tryNext());
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
        @Id
        private ObjectId id;
        private GeometryCollection everything;
        private String name;

        private AllTheThings() {
        }

        private AllTheThings(final String name, final GeometryCollection everything) {
            this.name = name;
            this.everything = everything;
        }

        public ObjectId getId() {
            return id;
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
