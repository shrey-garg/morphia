package org.mongodb.morphia.geo;

import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.query.Shape;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.GeoJson.point;

/**
 * Although this tests the old legacy coordinate system of storing location, this set of tests shows the functionality that's available
 * with
 * these coordinates in later versions of the server that also support GeoJSON.  In order to get full geo querying functionality, you
 * should
 * use GeoJSON for storing your location not legacy co-ordinates.
 * <p/>
 * This test requires server version 2.4 or above as it uses $geoWithin.
 */
@Ignore("Defer fixing the geo tests until after the core is fixed")
public class LegacyCoordsWithWithinQueries extends TestBase {
    @Test
    public void shouldNotReturnAnyPointsIfNothingInsideCircle() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords point = new PlaceWithLegacyCoords(new double[]{1, 1}, "place");
        getDatastore().save(point);
        getDatastore().ensureIndexes();

        // when - search with circle that does not cover the only point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.center(point(2, 2), 0.5))
                                                          .get();
        // then
        assertThat(found, is(nullValue()));
    }

    @Test
    public void shouldNotReturnAnyValuesWhenTheQueryBoxDoesNotContainAnyPoints() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords point = new PlaceWithLegacyCoords(new double[]{1, 1}, "place");
        getDatastore().save(point);
        getDatastore().ensureIndexes();

        // when - search with a box that does not cover the point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.box(point(0, 0), point(0.5, 0.5)))
                                                          .get();
        // then
        assertThat(found, is(nullValue()));
    }

    @Test
    public void shouldNotReturnAnyValuesWhenTheQueryPolygonDoesNotContainAnyPoints() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords point = new PlaceWithLegacyCoords(new double[]{7.3, 9.2}, "place");
        getDatastore().save(point);
        getDatastore().ensureIndexes();

        // when - search with polygon that's nowhere near the given point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.polygon(point(0, 0),
                                                                         point(0, 5),
                                                                         point(2, 3),
                                                                         point(1, 0)))
                                                          .get();
        // then
        assertThat(found, is(nullValue()));
    }

    @Test
    public void shouldReturnAPointThatIsFullyWithinQueryPolygon() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords expectedPoint = new PlaceWithLegacyCoords(new double[]{1, 1}, "place");
        getDatastore().save(expectedPoint);
        getDatastore().ensureIndexes();

        // when - search with polygon that contains expected point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.polygon(point(0, 0),
                                                                         point(0, 5),
                                                                         point(2, 3),
                                                                         point(1, 0)))
                                                          .get();
        // then
        assertThat(found, is(expectedPoint));
    }

    @Test
    public void shouldReturnOnlyThePointsWithinTheGivenCircle() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords expectedPoint = new PlaceWithLegacyCoords(new double[]{1.1, 2.3}, "Near point");
        getDatastore().save(expectedPoint);
        final PlaceWithLegacyCoords otherPoint = new PlaceWithLegacyCoords(new double[]{3.1, 5.2}, "Further point");
        getDatastore().save(otherPoint);
        getDatastore().ensureIndexes();

        // when
        final List<PlaceWithLegacyCoords> found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                                .field("location")
                                                                .within(Shape.center(point(1, 2), 1.1))
                                                                .asList();

        // then
        assertThat(found.size(), is(1));
        assertThat(found.get(0), is(expectedPoint));
    }

    @Test
    public void shouldReturnPointOnBoundaryOfQueryCircle() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords expectedPoint = new PlaceWithLegacyCoords(new double[]{1, 1}, "place");
        getDatastore().save(expectedPoint);
        getDatastore().ensureIndexes();

        // when - search with circle with an edge that exactly covers the point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.center(point(0, 1), 1))
                                                          .get();
        // then
        assertThat(found, is(expectedPoint));
    }

    @Test
    public void shouldReturnPointOnBoundaryOfQueryCircleWithSphericalGeometry() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords expectedPoint = new PlaceWithLegacyCoords(new double[]{1, 1}, "place");
        getDatastore().save(expectedPoint);
        getDatastore().ensureIndexes();

        // when - search with circle with an edge that exactly covers the point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.centerSphere(point(0, 1), 1))
                                                          .get();
        // then
        assertThat(found, is(expectedPoint));
    }

    @Test
    public void shouldReturnPointThatIsFullyInsideTheQueryBox() {
        // given
        checkMinServerVersion(2.4);

        final PlaceWithLegacyCoords expectedPoint = new PlaceWithLegacyCoords(new double[]{1, 1}, "place");
        getDatastore().save(expectedPoint);
        getDatastore().ensureIndexes();

        // when - search with a box that covers the whole point
        final PlaceWithLegacyCoords found = getDatastore().find(PlaceWithLegacyCoords.class)
                                                          .field("location")
                                                          .within(Shape.box(point(0, 0), point(2, 2)))
                                                          .get();
        // then
        assertThat(found, is(expectedPoint));
    }
}
