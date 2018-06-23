package org.mongodb.morphia.query;

import com.mongodb.client.model.geojson.LineString;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.geo.AllTheThings;
import org.mongodb.morphia.geo.Area;
import org.mongodb.morphia.geo.City;
import org.mongodb.morphia.geo.Regions;
import org.mongodb.morphia.geo.Route;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.GeoJson.geometryCollection;
import static org.mongodb.morphia.geo.GeoJson.lineString;
import static org.mongodb.morphia.geo.GeoJson.multiPoint;
import static org.mongodb.morphia.geo.GeoJson.multiPolygon;
import static org.mongodb.morphia.geo.GeoJson.point;
import static org.mongodb.morphia.geo.GeoJson.polygon;
import static org.mongodb.morphia.geo.GeoJson.position;

@Ignore("Defer fixing the geo tests until after the core is fixed")
public class GeoIntersectsQueriesWithLineTest extends TestBase {
    @Test
    public void shouldFindAPointThatLiesOnTheQueryLine() {
        // given
        LineString spanishLine = lineString(position(37.40759155713022, -5.964911067858338),
            position(37.3753708, -5.9550582));
        City manchester = new City("Manchester", point(53.4722454, -2.2235922));
        getDatastore().save(manchester);
        City london = new City("London", point(51.5286416, -0.1015987));
        getDatastore().save(london);
        City sevilla = new City("Sevilla", point(37.3753708, -5.9550582));
        getDatastore().save(sevilla);

        getDatastore().ensureIndexes();

        // when
        List<City> matchingCity = getDatastore().find(City.class)
                                                .field("location")
                                                .intersects(spanishLine)
                                                .asList();

        // then
        assertThat(matchingCity.size(), is(1));
        assertThat(matchingCity.get(0), is(sevilla));
    }

    @Test
    public void shouldFindAreasThatALineCrosses() {
        // given
        Area sevilla = new Area("Spain",
            polygon(
                position(37.40759155713022, -5.964911067858338),
                position(37.40341208875179, -5.9643941558897495),
                position(37.40297396667302, -5.970452763140202),
                position(37.40759155713022, -5.964911067858338)));
        getDatastore().save(sevilla);
        Area newYork = new Area("New York",
            polygon(
                position(40.75981395319104, -73.98302106186748),
                position(40.7636824529618, -73.98049869574606),
                position(40.76962974853814, -73.97964206524193),
                position(40.75981395319104, -73.98302106186748)));
        getDatastore().save(newYork);
        Area london = new Area("London",
            polygon(
                position(51.507780365645885, -0.21786745637655258),
                position(51.50802478194237, -0.21474729292094707),
                position(51.5086863655597, -0.20895397290587425),
                position(51.507780365645885, -0.21786745637655258)));
        getDatastore().save(london);
        getDatastore().ensureIndexes();

        // when
        List<Area> areaContainingPoint = getDatastore().find(Area.class)
                                                       .field("area")
                                                       .intersects(lineString(
                                                           position(37.4056048, -5.9666089),
                                                           position(37.404497, -5.9640557)))
                                                       .asList();

        // then
        assertThat(areaContainingPoint.size(), is(1));
        assertThat(areaContainingPoint.get(0), is(sevilla));
    }

    @Test
    public void shouldFindGeometryCollectionsWhereTheGivenPointIntersectsWithOneOfTheEntities() {
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
        List<AllTheThings> everythingInTheUK = getDatastore().find(AllTheThings.class)
                                                             .field("everything")
                                                             .intersects(lineString(position(37.4056048, -5.9666089),
                                                                 position(37.404497, -5.9640557)))
                                                             .asList();

        // then
        assertThat(everythingInTheUK.size(), is(1));
        assertThat(everythingInTheUK.get(0), is(sevilla));
    }

    @Test
    public void shouldFindRegionsThatALineCrosses() {
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
        List<Regions> regionsInTheUK = getDatastore().find(Regions.class)
                                                     .field("regions")
                                                     .intersects(lineString(position(37.4056048, -5.9666089),
                                                         position(37.404497, -5.9640557)))
                                                     .asList();

        // then
        assertThat(regionsInTheUK.size(), is(1));
        assertThat(regionsInTheUK.get(0), is(sevilla));
    }

    @Test
    public void shouldFindRoutesThatALineCrosses() {
        // given
        Route sevilla = new Route("Spain", lineString(position(37.4045286, -5.9642332),
            position(37.4061095, -5.9645765)));
        getDatastore().save(sevilla);
        Route newYork = new Route("New York", lineString(position(40.75981395319104, -73.98302106186748),
            position(40.7636824529618, -73.98049869574606),
            position(40.76962974853814, -73.97964206524193)));
        getDatastore().save(newYork);
        Route london = new Route("London", lineString(position(51.507780365645885, -0.21786745637655258),
            position(51.50802478194237, -0.21474729292094707),
            position(51.5086863655597, -0.20895397290587425)));
        getDatastore().save(london);
        Route londonToParis = new Route("London To Paris", lineString(position(51.5286416, -0.1015987),
            position(48.858859, 2.3470599)));
        getDatastore().save(londonToParis);
        getDatastore().ensureIndexes();

        // when
        List<Route> routeContainingPoint = getDatastore().find(Route.class)
                                                         .field("route")
                                                         .intersects(lineString(position(37.4043709, -5.9643244),
                                                             position(37.4045286, -5.9642332)))
                                                         .asList();

        // then
        assertThat(routeContainingPoint.size(), is(1));
        assertThat(routeContainingPoint.get(0), is(sevilla));
    }

}
