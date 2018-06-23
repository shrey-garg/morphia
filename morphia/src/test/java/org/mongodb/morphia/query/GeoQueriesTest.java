package org.mongodb.morphia.query;

import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.TestBase;
import org.mongodb.morphia.geo.City;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.GeoJson.point;

public class GeoQueriesTest extends TestBase {
    @Test
    public void shouldFindCitiesOrderByDistanceFromAGivenPoint() {
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
        List<City> citiesOrderedByDistanceFromLondon = getDatastore().find(City.class)
                                                                     .field("location")
                                                                     .near(point(latitudeLondon, longitudeLondon))
                                                                     .asList();

        // then
        assertThat(citiesOrderedByDistanceFromLondon.size(), is(3));
        assertThat(citiesOrderedByDistanceFromLondon.get(0), is(london));
        assertThat(citiesOrderedByDistanceFromLondon.get(1), is(manchester));
        assertThat(citiesOrderedByDistanceFromLondon.get(2), is(sevilla));
    }

}
