package org.mongodb.morphia.query;

import org.junit.Ignore;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mongodb.morphia.geo.GeoJson.polygon;
import static org.mongodb.morphia.geo.GeoJson.position;

/**
 * Unit test - more complete testing that uses the GeoJson factory is contained in functional Geo tests.
 */
@Ignore("Defer fixing the geo tests until after the core is fixed")
public class GeoJsonTest {
    @Test(expected = IllegalArgumentException.class)
    public void shouldErrorIfStartAndEndOfPolygonAreNotTheSame() {
        // expect
        polygon(
            position(1.1, 2.0),
            position(2.3, 3.5),
            position(3.7, 1.0));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldNotErrorIfPolygonIsEmpty() {
        // expect
        assertThat(polygon(emptyList()), is(notNullValue()));
    }

}
