package io.confluent.strata;

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
            System.err.printf("name=%s, value=%s\n",p.getName(),p.getValue());
            if (p.getName().toString().equals("NAME"))
                that.name = p.getValue().toString();
            if (p.getName().toString().equals("STATE"))
                that.state = p.getValue().toString();

            if (p.getName().toString().equals("COUNTY"))
                that.county = p.getValue().toString();

            if (p.getName().toString().equals("CITY"))
                that.city = p.getValue().toString();
        }

        System.out.printf("%s %s\n", feature.getID(), that);
        System.out.printf("geom = [%s] %s\n\n", feature.getDefaultGeometry().getClass().getName(),
                feature.getDefaultGeometry());

        that.multiPolygon = (MultiPolygon) feature.getDefaultGeometry();
        return that;
    }
}