package org.mongodb.morphia.query;


import com.mongodb.client.model.geojson.Point;

/**
 * This encapsulates the data necessary to define a shape for queries.
 */
public class Shape {
    private final String geometry;
    private final Point[] points;

    Shape(final String geometry, final Point... points) {
        this.geometry = geometry;
        this.points = points;
    }

    /**
     * Specifies a rectangle for a geospatial $geoWithin query to return documents that are within the bounds of the rectangle,
     * according to their point-based location data.
     *
     * @param bottomLeft the bottom left bound
     * @param upperRight the upper right bound
     * @return the box
     * @mongodb.driver.manual reference/operator/query/box/ $box
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     */
    public static Shape box(final Point bottomLeft, final Point upperRight) {
        return new Shape("$box", bottomLeft, upperRight);
    }

    /**
     * Specifies a circle for a $geoWithin query.
     *
     * @param center the center of the circle
     * @param radius the radius circle
     * @return the box
     * @mongodb.driver.manual reference/operator/query/center/ $center
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     */
    public static Shape center(final Point center, final double radius) {
        return new Center("$center", center, radius);
    }

    /**
     * Specifies a circle for a geospatial query that uses spherical geometry.
     *
     * @param center the center of the circle
     * @param radius the radius circle
     * @return the box
     * @mongodb.driver.manual reference/operator/query/centerSphere/ $centerSphere
     */
    public static Shape centerSphere(final Point center, final double radius) {
        return new Center("$centerSphere", center, radius);
    }

    /**
     * Specifies a polygon for a geospatial $geoWithin query on legacy coordinate pairs.
     *
     * @param points the points of the polygon
     * @return the box
     * @mongodb.driver.manual reference/operator/query/polygon/ $polygon
     * @mongodb.driver.manual reference/operator/query/geoWithin/ $geoWithin
     */
    public static Shape polygon(final Point... points) {
        return new Shape("$polygon", points);
    }

    private static class Center extends Shape {
        private final Point center;
        private final double radius;

        Center(final String geometry, final Point center, final double radius) {
            super(geometry);
            this.center = center;
            this.radius = radius;
        }
    }
}
