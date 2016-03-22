package io.confluent.strata;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.geotools.data.*;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by jadler on 3/17/16.
 */
public class ReverseGeocoder implements ValueMapper<GenericRecord, GenericRecord>  {

    SpatialIndex qt = new STRtree();
    String latitudeKey = null;
    String longitudeKey = null;
    String locationNameKey = null;

    public ReverseGeocoder(Collection<String> shapefiles)  {
        super();
        shapefiles.forEach(file -> loadShapeFile(file));
    }

    public ReverseGeocoder(Collection<String> shapefiles, String initialLatitudeKey,
                           String initialLongitudeKey, String initialLocationName)  {
        super();
        latitudeKey = initialLatitudeKey;
        longitudeKey = initialLongitudeKey;
        locationNameKey = initialLocationName;
        shapefiles.forEach(file -> loadShapeFile(file));
    }

    public void loadShapeFile(String shapefileName)  {
        try {
            File shapefile = new File(shapefileName);
            Map<String, Object> shapefileParams = Maps.newHashMap();
            shapefileParams.put("url", shapefile.toURI().toURL());
            DataStore dataStore = DataStoreFinder.getDataStore(shapefileParams);
            if (dataStore == null)
                throw new RuntimeException("couldn't load the damn data store: " + shapefile);
            String typeName = dataStore.getTypeNames()[0];
            FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore.getFeatureSource(typeName);
            Filter filter = Filter.INCLUDE;
            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);

            try (FeatureIterator<SimpleFeature> features = collection.features()) {
                while (features.hasNext()) {
                    SimpleFeature feature = features.next();
                    GeoInfo geoInfo = GeoInfo.fromSimpleFeature(feature);
                    qt.insert(GeoUtils.getBoundingRectangleAsEnvelope(geoInfo.multiPolygon),
                            geoInfo);
                }
            }
            System.err.printf("loaded shapefile %s\n", shapefile);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private GeometryFactory geometryFactory = new GeometryFactory();

    public GeoInfo findGeoInfoForPoint(double y, double x) {
        Coordinate coordinate = new Coordinate(x,y);
        Geometry geometry = geometryFactory.createPoint(coordinate);
        List<GeoInfo> candidates =  qt.query(new Envelope(coordinate));
        // System.err.printf("Looking at a list of %d candidates for coordinate %s\n", candidates.size(), coordinate);
        for (GeoInfo candidate: candidates) {
            if (candidate.multiPolygon.contains(geometry))
                return candidate;
        }
        return null;
    }

    public List<GeoInfo> findAllGeoInfoForPoint(double y, double x) {
        Coordinate coordinate = new Coordinate(x,y);
        Geometry geometry = geometryFactory.createPoint(coordinate);
        List<GeoInfo> candidates =  qt.query(new Envelope(coordinate));
        // System.err.printf("Looking at a list of %d candidates\n", candidates.size());
        List<GeoInfo> stuffFound = Lists.newArrayList();
        for (GeoInfo candidate: candidates) {
            if (candidate.multiPolygon.contains(geometry))
                stuffFound.add(candidate);
        }
        return stuffFound;
    }

    Schema schema = null;

    @Override
    public GenericRecord apply(GenericRecord genericRecord) {
        if (schema == null) {
            schema = AvroUtils.addFieldsToSchema(
                    genericRecord.getSchema(),
                    Lists.newArrayList(new Schema.Field(
                            locationNameKey,
                            Schema.createUnion(
                                    Lists.newArrayList(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL))),
                            null,
                            null)));
        }

        Double latitude = (Double) genericRecord.get(latitudeKey);
        Double longitude = (Double) genericRecord.get(longitudeKey);
        GeoInfo geoInfo = findGeoInfoForPoint(latitude, longitude);
        String neighborhood = null;
        if (geoInfo != null)
             neighborhood = findGeoInfoForPoint(latitude, longitude).name;
        GenericData.Record newRecord = AvroUtils.copyRecord(genericRecord, schema);
        newRecord.put(locationNameKey, neighborhood);
        System.err.printf("%f %f => %s\n", latitude, longitude, neighborhood==null?"null":neighborhood);
        return newRecord;
    }
}
