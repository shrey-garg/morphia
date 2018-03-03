package org.mongodb.morphia.geo;

import com.mongodb.client.model.geojson.Position;

/**
 * Creates Position instances representing a <a href="http://docs.mongodb.org/manual/apps/geospatial-indexes/#geojson-objects">GeoJSON</a>
 * Position type. The advantage of using the builder is to reduce confusion of the order of the latitude and longitude double values.
 * <p/>
 * Supported by server versions 2.4 and above.
 *
 * @see Position
 */
public class PositionBuilder {
    private double longitude;
    private double latitude;

    /**
     * Convenience method to return a new PositionBuilder.
     *
     * @return a new instance of PositionBuilder.
     */
    public static PositionBuilder builder() {
        return new PositionBuilder();
    }

    /**
     * Creates an immutable Position
     *
     * @return the Position with the specifications from this builder.
     */
    public Position build() {
        return new Position(latitude, longitude);
    }

    /**
     * Add a latitude.
     *
     * @param latitude the latitude of the Position
     * @return this PositionBuilder
     */
    public PositionBuilder latitude(final double latitude) {
        this.latitude = latitude;
        return this;
    }

    /**
     * Add a longitude.
     *
     * @param longitude the longitude of the Position
     * @return this PositionBuilder
     */
    public PositionBuilder longitude(final double longitude) {
        this.longitude = longitude;
        return this;
    }
}
