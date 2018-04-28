package org.mongodb.morphia.query;


import com.mongodb.MongoException;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Indexed;
import org.mongodb.morphia.utils.IndexDirection;

import static org.mongodb.morphia.geo.GeoJson.point;


@Ignore("Defer fixing the geo tests until after the core is fixed")
public class TestGeoQueries extends TestBase {
    @Override
    public void setUp() {
        super.setUp();
        getMorphia().map(Place.class);
    }

    @Test
    public void testGeoWithinPolygon() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{0, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.polygon(point(0, 0), point(0, 5), point(2, 3), point(2, 0)))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testGeoWithinPolygon2() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{10, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.polygon(point(0, 0), point(0, 5), point(2, 3), point(2, 0)))
                                          .get();
        Assert.assertNull(found);
    }

    @Test
    public void testGeoWithinRadius() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.center(point(0, 1), 1.1))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testGeoWithinRadius2() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.center(point(0.5, 0.5), 0.77))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testGeoWithinRadiusSphere() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.centerSphere(point(0, 1), 1))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testNear() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .near(0, 0)
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testNearMaxDistance() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .near(0, 0, 1.5)
                                          .get();
        Assert.assertNotNull(found);
        final Place notFound = getDatastore().find(Place.class)
                                             .field("loc")
                                             .near(0, 0, 1)
                                             .get();
        Assert.assertNull(notFound);
    }

    @Test
    public void testNearNoIndex() {
        checkMinServerVersion(2.4);
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        Place found = null;
        try {
            found = getDatastore().find(Place.class)
                                  .field("loc")
                                  .near(0, 0)
                                  .get();
            Assert.assertFalse(true);
        } catch (MongoException e) {
            Assert.assertNull(found);
        }
    }

    @Test
    public void testWithinBox() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.box(point(0, 0), point(2, 2)))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinOutsideBox() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.box(point(0, 0), point(.4, .5)))
                                          .get();
        Assert.assertNull(found);
    }

    @Test
    public void testWithinOutsideRadius() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.center(point(2, 2), .4))
                                          .get();
        Assert.assertNull(found);
    }

    @Test
    public void testWithinRadius() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.center(point(0, 1), 1.1))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinRadius2() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.center(point(0.5, 0.5), 0.77))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Test
    public void testWithinRadiusSphere() {
        checkMinServerVersion(2.4);
        getDatastore().ensureIndexes();
        final Place place1 = new Place("place1", new double[]{1, 1});
        getDatastore().save(place1);
        final Place found = getDatastore().find(Place.class)
                                          .field("loc")
                                          .within(Shape.centerSphere(point(0, 1), 1))
                                          .get();
        Assert.assertNotNull(found);
    }

    @Entity
    private static class Place {
        @Id
        private ObjectId id;
        private String name;
        @Indexed(IndexDirection.GEO2D)
        private double[] loc;

        private Place() {
        }

        Place(final String name, final double[] loc) {
            this.name = name;
            this.loc = loc;
        }
    }
}
