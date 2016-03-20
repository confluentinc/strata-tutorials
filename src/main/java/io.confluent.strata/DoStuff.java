package io.confluent.strata;

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;

/**
 * Created by jadler on 3/17/16.
 */
public class DoStuff {

    public static void main(String argv[])  {

        File nyShapefile = new File ("data/ZillowNeighborhoods-NY/ZillowNeighborhoods-NY.shp");
        File njShapefile = new File ("data/ZillowNeighborhoods-NJ/ZillowNeighborhoods-NJ.shp");

        ReverseGeocoder reverseGeocoder =
                new ReverseGeocoder(
                        Lists.newArrayList(nyShapefile, njShapefile));

        //penn station
        //y 40.7515906, x -74.0045378
        System.out.printf("%s\n", reverseGeocoder.findAllGeoInfoForPoint(-74.0045378, 40.7515906));

        // upper west side
        //y 40.7634559,x -73.994967
        System.out.printf("%s\n", reverseGeocoder.findAllGeoInfoForPoint(-73.994967, 40.7634559));

        // lincoln tunnel
        // y 40.759588, x -74.002220
        System.out.printf("%s\n", reverseGeocoder.findAllGeoInfoForPoint(-74.002220, 40.759588));

    }
}
