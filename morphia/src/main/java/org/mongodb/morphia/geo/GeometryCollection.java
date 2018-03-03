package org.mongodb.morphia.geo;

import org.mongodb.morphia.annotations.Embedded;
import org.mongodb.morphia.annotations.Entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class represents a collection of mixed GeoJson objects as per the <a href="http://geojson.org/geojson-spec
 * .html#geometrycollection">GeoJSON
 * specification</a>. Therefore this entity will never have its own ID or store the its Class name.
 * <p/>
 * The factory for creating a MultiPoint is the {@code GeoJson.multiPoint} method.
 *
 * @see org.mongodb.morphia.geo.GeoJson
 */
@Embedded
@Entity(noClassnameStored = true)
public class GeometryCollection {
    private final String type = "GeometryCollection";
    private final List<Geometry> geometries;

    @SuppressWarnings("UnusedDeclaration") // needed by morphia
    private GeometryCollection() {
        geometries = new ArrayList<>();
    }

    GeometryCollection(final List<Geometry> geometries) {
        this.geometries = geometries;
    }

    GeometryCollection(final Geometry... geometries) {
        this.geometries = Arrays.asList(geometries);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final GeometryCollection that = (GeometryCollection) o;

        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }
        return geometries != null ? geometries.equals(that.geometries) : that.geometries == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (geometries != null ? geometries.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GeometryCollection{"
               + "type='" + type + '\''
               + ", geometries=" + geometries
               + '}';
    }
}
