package io.confluent.strata.geo;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.MultiPolygon;

/**
 * Created by jadler on 3/17/16.
 * Class of static utility functions for doing geo stuff
 */
public class GeoUtils {

    static Envelope getBoundingRectangleAsEnvelope(MultiPolygon multiPolygon) {
        double minX = 180.0;
        double maxX = -180.0;
        double minY = 180.0;
        double maxY = -180.0;
        for (Coordinate coordinate : multiPolygon.getCoordinates()) {
            minX = Math.min(minX, coordinate.x);
            maxX = Math.max(maxX, coordinate.x);
            minY = Math.min(minY, coordinate.y);
            maxY = Math.max(maxY, coordinate.y);
        }
        return new Envelope(minX, maxX, minY, maxY);
    }
}
