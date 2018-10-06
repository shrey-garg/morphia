package xyz.morphia.query;

import org.junit.Ignore;
import org.junit.Test;
import xyz.morphia.Datastore;
import xyz.morphia.TestBase;
import xyz.morphia.geo.AllTheThings;
import xyz.morphia.geo.Area;
import xyz.morphia.geo.City;
import xyz.morphia.geo.Regions;
import xyz.morphia.geo.Route;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static xyz.morphia.geo.GeoJson.geometryCollection;
import static xyz.morphia.geo.GeoJson.lineString;
import static xyz.morphia.geo.GeoJson.multiPoint;
import static xyz.morphia.geo.GeoJson.multiPolygon;
import static xyz.morphia.geo.GeoJson.point;
import static xyz.morphia.geo.GeoJson.polygon;
import static xyz.morphia.geo.GeoJson.position;

@Ignore("Defer fixing the geo tests until after the core is fixed")
public class GeoNearQueriesTest extends TestBase {
    @Test
    public void shouldFindAreasCloseToAGivenPointWithinARadiusOfMeters() {
        // given
        Area sevilla = new Area("Spain", polygon(
            point(37.40759155713022, -5.964911067858338),
            point(37.40341208875179, -5.9643941558897495),
            point(37.40297396667302, -5.970452763140202),
            point(37.40759155713022, -5.964911067858338))
        );
        getDatastore().save(sevilla);
        Area newYork = new Area("New York", polygon(
            point(40.75981395319104, -73.98302106186748),
            point(40.7636824529618, -73.98049869574606),
            point(40.76962974853814, -73.97964206524193),
            point(40.75981395319104, -73.98302106186748)));
        getDatastore().save(newYork);
        Area london = new Area("London", polygon(
            point(51.507780365645885, -0.21786745637655258),
            point(51.50802478194237, -0.21474729292094707),
            point(51.5086863655597, -0.20895397290587425),
            point(51.507780365645885, -0.21786745637655258)));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Area> routesOrderedByDistanceFromLondon = getDatastore().find(Area.class)
                                                                     .field("area")
                                                                     .near(point(51.5286416, -0.1015987), 20000)
                                                                     .asList();

        // then
        assertThat(routesOrderedByDistanceFromLondon.size(), is(1));
        assertThat(routesOrderedByDistanceFromLondon.get(0), is(london));
    }

    @Test
    public void shouldFindAreasOrderedByDistanceFromAGivenPoint() {
        // given
        Area sevilla = new Area("Spain", polygon(
            point(37.40759155713022, -5.964911067858338),
            point(37.40341208875179, -5.9643941558897495),
            point(37.40297396667302, -5.970452763140202),
            point(37.40759155713022, -5.964911067858338))
        );
        getDatastore().save(sevilla);
        Area newYork = new Area("New York", polygon(
            point(40.75981395319104, -73.98302106186748),
            point(40.7636824529618, -73.98049869574606),
            point(40.76962974853814, -73.97964206524193),
            point(40.75981395319104, -73.98302106186748)));
        getDatastore().save(newYork);
        Area london = new Area("London", polygon(
            point(51.507780365645885, -0.21786745637655258),
            point(51.50802478194237, -0.21474729292094707),
            point(51.5086863655597, -0.20895397290587425),
            point(51.507780365645885, -0.21786745637655258)));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Area> routesOrderedByDistanceFromLondon = getDatastore().find(Area.class)
                                                                     .field("area")
                                                                     .near(point(51.5286416, -0.1015987))
                                                                     .asList();

        // then
        assertThat(routesOrderedByDistanceFromLondon.size(), is(3));
        assertThat(routesOrderedByDistanceFromLondon.get(0), is(london));
        assertThat(routesOrderedByDistanceFromLondon.get(1), is(sevilla));
        assertThat(routesOrderedByDistanceFromLondon.get(2), is(newYork));
    }

    @Test
    public void shouldFindCitiesCloseToAGivenPointWithinARadiusOfMeters() {
        // given
        double latitude = 51.5286416;
        double longitude = -0.1015987;
        Datastore datastore = getDatastore();
        City london = new City("London", point(latitude, longitude));
        datastore.save(london);
        City manchester = new City("Manchester", point(53.4722454, -2.2235922));
        datastore.save(manchester);
        City sevilla = new City("Sevilla", point(37.3753708, -5.9550582));
        datastore.save(sevilla);

        getDatastore().ensureIndexes();

        // when
        List<City> citiesOrderedByDistanceFromLondon = datastore.find(City.class)
                                                                .field("location")
                                                                .near(point(latitude, longitude), 200000)
                                                                .asList();

        // then
        assertThat(citiesOrderedByDistanceFromLondon.size(), is(1));
        assertThat(citiesOrderedByDistanceFromLondon.get(0), is(london));
    }

    @Test
    public void shouldFindCitiesOrderedByDistanceFromAGivenPoint() {
        // given
        double latitudeLondon = 51.5286416;
        double longitudeLondon = -0.1015987;
        City manchester = new City("Manchester", point(53.4722454, -2.2235922));
        getDatastore().save(manchester);
        City london = new City("London", point(latitudeLondon, longitudeLondon));
        getDatastore().save(london);
        City sevilla = new City("Sevilla", point(37.3753708, -5.9550582));
        getDatastore().save(sevilla);

        getDatastore().ensureIndexes();

        // when
        List<City> citiesOrderedByDistanceFromLondon;
        citiesOrderedByDistanceFromLondon = getDatastore().find(City.class)
                                                          .field("location")
                                                          .near(point(latitudeLondon, longitudeLondon))
                                                          .asList();

        // then
        assertThat(citiesOrderedByDistanceFromLondon.size(), is(3));
        assertThat(citiesOrderedByDistanceFromLondon.get(0), is(london));
        assertThat(citiesOrderedByDistanceFromLondon.get(1), is(manchester));
        assertThat(citiesOrderedByDistanceFromLondon.get(2), is(sevilla));
    }

    @Test
    public void shouldFindGeometryCollectionsCloseToAGivenPointWithinARadiusOfMeters() {
        AllTheThings sevilla = new AllTheThings("Spain", geometryCollection(
            multiPoint(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202)),
            polygon(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202),
                position(37.40759155713022, -5.964911067858338)),
            polygon(
                position(37.38744598813355, -6.001141928136349),
                position(37.385990973562, -6.002588979899883),
                position(37.386126928031445, -6.002463921904564),
                position(37.38744598813355, -6.001141928136349))));
        getDatastore().save(sevilla);

        // insert something that's not a geocollection
        Regions usa = new Regions("US", multiPolygon(
            polygon(
                position(40.75981395319104, -73.98302106186748),
                position(40.7636824529618, -73.98049869574606),
                position(40.76962974853814, -73.97964206524193),
                position(40.75981395319104, -73.98302106186748)),
            polygon(
                position(28.326568258926272, -81.60542246885598),
                position(28.327541397884488, -81.6022228449583),
                position(28.32950334995985, -81.60564735531807),
                position(28.326568258926272, -81.60542246885598))));
        getDatastore().save(usa);

        AllTheThings london = new AllTheThings("London", geometryCollection(
            point(53.4722454, -2.2235922),
            lineString(
                position(51.507780365645885, -0.21786745637655258),
                position(51.50802478194237, -0.21474729292094707),
                position(51.5086863655597, -0.20895397290587425)),
            polygon(
                position(51.498216362670064, 0.0074849557131528854),
                position(51.49176875129342, 0.01821178011596203),
                position(51.492886897176504, 0.05523204803466797),
                position(51.49393044412136, 0.06663135252892971),
                position(51.498216362670064, 0.0074849557131528854))));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<AllTheThings> resultsOrderedByDistanceFromSeville = getDatastore().find(AllTheThings.class)
                                                                               .field("everything")
                                                                               .near(point(37.3753707, -5.9550583), 20000)
                                                                               .asList();

        // then
        assertThat(resultsOrderedByDistanceFromSeville.size(), is(1));
        assertThat(resultsOrderedByDistanceFromSeville.get(0), is(sevilla));
    }

    @Test
    public void shouldFindGeometryCollectionsOrderedByDistanceFromAGivenPoint() {
        AllTheThings sevilla = new AllTheThings("Spain", geometryCollection(
            multiPoint(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202)),
            polygon(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202),
                position(37.40759155713022, -5.964911067858338)),
            polygon(
                position(37.38744598813355, -6.001141928136349),
                position(37.385990973562, -6.002588979899883),
                position(37.386126928031445, -6.002463921904564),
                position(37.38744598813355, -6.001141928136349))));
        getDatastore().save(sevilla);

        // insert something that's not a geocollection
        Regions usa = new Regions("US", multiPolygon(
            polygon(
                position(40.75981395319104, -73.98302106186748),
                position(40.7636824529618, -73.98049869574606),
                position(40.76962974853814, -73.97964206524193),
                position(40.75981395319104, -73.98302106186748)),
            polygon(
                position(28.326568258926272, -81.60542246885598),
                position(28.327541397884488, -81.6022228449583),
                position(28.32950334995985, -81.60564735531807),
                position(28.326568258926272, -81.60542246885598))));
        getDatastore().save(usa);

        AllTheThings london = new AllTheThings("London", geometryCollection(
            point(53.4722454, -2.2235922),
            lineString(
                position(51.507780365645885, -0.21786745637655258),
                position(51.50802478194237, -0.21474729292094707),
                position(51.5086863655597, -0.20895397290587425)),
            polygon(
                position(51.498216362670064, 0.0074849557131528854),
                position(51.49176875129342, 0.01821178011596203),
                position(51.492886897176504, 0.05523204803466797),
                position(51.49393044412136, 0.06663135252892971),
                position(51.498216362670064, 0.0074849557131528854))));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<AllTheThings> resultsOrderedByDistanceFromLondon = getDatastore().find(AllTheThings.class)
                                                                              .field("everything")
                                                                              .near(point(51.5286416, -0.1015987))
                                                                              .asList();

        // then
        assertThat(resultsOrderedByDistanceFromLondon.size(), is(2));
        assertThat(resultsOrderedByDistanceFromLondon.get(0), is(london));
        assertThat(resultsOrderedByDistanceFromLondon.get(1), is(sevilla));
    }

    @Test
    public void shouldFindRegionsCloseToAGivenPointWithinARadiusOfMeters() {
        Regions sevilla = new Regions("Spain", multiPolygon(
            polygon(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202),
                position(37.40759155713022, -5.964911067858338)),
            polygon(
                position(37.38744598813355, -6.001141928136349),
                position(37.385990973562, -6.002588979899883),
                position(37.386126928031445, -6.002463921904564),
                position(37.38744598813355, -6.001141928136349))));
        getDatastore().save(sevilla);

        Regions usa = new Regions("US", multiPolygon(
            polygon(
                position(40.75981395319104, -73.98302106186748),
                position(40.7636824529618, -73.98049869574606),
                position(40.76962974853814, -73.97964206524193),
                position(40.75981395319104, -73.98302106186748)),
            polygon(
                position(28.326568258926272, -81.60542246885598),
                position(28.327541397884488, -81.6022228449583),
                position(28.32950334995985, -81.60564735531807),
                position(28.326568258926272, -81.60542246885598))));
        getDatastore().save(usa);

        Regions london = new Regions("London", multiPolygon(
            polygon(
                position(51.507780365645885, -0.21786745637655258),
                position(51.50802478194237, -0.21474729292094707),
                position(51.5086863655597, -0.20895397290587425),
                position(51.507780365645885, -0.21786745637655258)),
            polygon(
                position(51.498216362670064, 0.0074849557131528854),
                position(51.49176875129342, 0.01821178011596203),
                position(51.492886897176504, 0.05523204803466797),
                position(51.49393044412136, 0.06663135252892971),
                position(51.498216362670064, 0.0074849557131528854))));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Regions> regionsOrderedByDistanceFromLondon = getDatastore().find(Regions.class)
                                                                         .field("regions")
                                                                         .near(point(51.5286416, -0.1015987), 20000)
                                                                         .asList();

        // then
        assertThat(regionsOrderedByDistanceFromLondon.size(), is(1));
        assertThat(regionsOrderedByDistanceFromLondon.get(0), is(london));
    }

    @Test
    public void shouldFindRegionsOrderedByDistanceFromAGivenPoint() {
        Regions sevilla = new Regions("Spain", multiPolygon(
            polygon(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202),
                position(37.40759155713022, -5.964911067858338)),
            polygon(
                position(37.38744598813355, -6.001141928136349),
                position(37.385990973562, -6.002588979899883),
                position(37.386126928031445, -6.002463921904564),
                position(37.38744598813355, -6.001141928136349))));
        getDatastore().save(sevilla);

        Regions usa = new Regions("US", multiPolygon(
            polygon(
                position(40.75981395319104, -73.98302106186748),
                position(40.7636824529618, -73.98049869574606),
                position(40.76962974853814, -73.97964206524193),
                position(40.75981395319104, -73.98302106186748)),
            polygon(
                position(28.326568258926272, -81.60542246885598),
                position(28.327541397884488, -81.6022228449583),
                position(28.32950334995985, -81.60564735531807),
                position(28.326568258926272, -81.60542246885598))));
        getDatastore().save(usa);

        Regions london = new Regions("London", multiPolygon(
            polygon(
                position(51.507780365645885, -0.21786745637655258),
                position(51.50802478194237, -0.21474729292094707),
                position(51.5086863655597, -0.20895397290587425),
                position(51.507780365645885, -0.21786745637655258)),
            polygon(
                position(51.498216362670064, 0.0074849557131528854),
                position(51.49176875129342, 0.01821178011596203),
                position(51.492886897176504, 0.05523204803466797),
                position(51.49393044412136, 0.06663135252892971),
                position(51.498216362670064, 0.0074849557131528854))));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Regions> regionsOrderedByDistanceFromLondon = getDatastore().find(Regions.class)
                                                                         .field("regions")
                                                                         .near(point(51.5286416, -0.1015987))
                                                                         .asList();

        // then
        assertThat(regionsOrderedByDistanceFromLondon.size(), is(3));
        assertThat(regionsOrderedByDistanceFromLondon.get(0), is(london));
        assertThat(regionsOrderedByDistanceFromLondon.get(1), is(sevilla));
        assertThat(regionsOrderedByDistanceFromLondon.get(2), is(usa));
    }

    @Test
    public void shouldFindRoutesCloseToAGivenPointWithinARadiusOfMeters() {
        // given
        Route sevilla = new Route("Spain", lineString(
            position(37.40759155713022, -5.964911067858338),
            position(37.40341208875179, -5.9643941558897495),
            position(37.40297396667302, -5.970452763140202)));
        getDatastore().save(sevilla);
        Route newYork = new Route("New York", lineString(
            position(40.75981395319104, -73.98302106186748),
            position(40.7636824529618, -73.98049869574606),
            position(40.76962974853814, -73.97964206524193)));
        getDatastore().save(newYork);
        Route london = new Route("London", lineString(
            position(51.507780365645885, -0.21786745637655258),
            position(51.50802478194237, -0.21474729292094707),
            position(51.5086863655597, -0.20895397290587425)));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Route> routesOrderedByDistanceFromLondon = getDatastore().find(Route.class)
                                                                      .field("route")
                                                                      .near(point(51.5286416, -0.1015987), 20000)
                                                                      .asList();

        // then
        assertThat(routesOrderedByDistanceFromLondon.size(), is(1));
        assertThat(routesOrderedByDistanceFromLondon.get(0), is(london));
    }

    @Test
    public void shouldFindRoutesOrderedByDistanceFromAGivenPoint() {
        // given
        Route sevilla = new Route("Spain", lineString(
            position(37.40759155713022, -5.964911067858338),
            position(37.40341208875179, -5.9643941558897495),
            position(37.40297396667302, -5.970452763140202)));
        getDatastore().save(sevilla);
        Route newYork = new Route("New York", lineString(
            position(40.75981395319104, -73.98302106186748),
            position(40.7636824529618, -73.98049869574606),
            position(40.76962974853814, -73.97964206524193)));
        getDatastore().save(newYork);
        Route london = new Route("London", lineString(
            position(51.507780365645885, -0.21786745637655258),
            position(51.50802478194237, -0.21474729292094707),
            position(51.5086863655597, -0.20895397290587425)));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Route> routesOrderedByDistanceFromLondon = getDatastore().find(Route.class)
                                                                      .field("route")
                                                                      .near(point(51.5286416, -0.1015987))
                                                                      .asList();

        // then
        assertThat(routesOrderedByDistanceFromLondon.size(), is(3));
        assertThat(routesOrderedByDistanceFromLondon.get(0), is(london));
        assertThat(routesOrderedByDistanceFromLondon.get(1), is(sevilla));
        assertThat(routesOrderedByDistanceFromLondon.get(2), is(newYork));
    }

}
