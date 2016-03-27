package io.confluent.strata.geo;

import com.vividsolutions.jts.geom.MultiPolygon;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Created by jadler on 3/17/16.
 */
class GeoInfo {
    MultiPolygon multiPolygon;
    String name;
    String state;
    String county;
    String city;

    @Override
    public String toString() {
        return name + ":" + state + ":" + county + ":" + city;
    }

    public static GeoInfo fromSimpleFeature(SimpleFeature feature) {
        GeoInfo that = new GeoInfo();
        for (Property p: feature.getProperties()) {
            if (p.getName().toString().equals("NAME"))
                that.name = p.getValue().toString();
            if (p.getName().toString().equals("STATE"))
                that.state = p.getValue().toString();

            if (p.getName().toString().equals("COUNTY"))
                that.county = p.getValue().toString();

            if (p.getName().toString().equals("CITY"))
                that.city = p.getValue().toString();
        }

        that.multiPolygon = (MultiPolygon) feature.getDefaultGeometry();
        return that;
    }
}