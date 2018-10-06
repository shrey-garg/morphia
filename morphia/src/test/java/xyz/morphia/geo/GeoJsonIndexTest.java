package xyz.morphia.geo;

import com.mongodb.client.model.geojson.Point;
import org.junit.Test;
import xyz.morphia.TestBase;
import xyz.morphia.annotations.Indexed;
import xyz.morphia.utils.IndexType;

import static xyz.morphia.geo.GeoJson.point;

public class GeoJsonIndexTest extends TestBase {
    @Test(expected = Exception.class)
    public void shouldErrorWhenCreatingA2dIndexOnGeoJson() {
        // given
        Place pointB = new Place(point(3.1, 7.5), "Point B");
        getDatastore().save(pointB);

        // when
        getDatastore().ensureIndexes();
        //"location object expected, location array not in correct format", code : 13654
    }

    @SuppressWarnings("unused")
    private static final class Place {
        @Indexed(IndexType.GEO2D)
        private Point location;
        private String name;

        private Place(final Point location, final String name) {
            this.location = location;
            this.name = name;
        }

        private Place() {
        }
    }
}
