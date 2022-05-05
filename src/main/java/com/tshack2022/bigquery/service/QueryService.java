package com.tshack2022.bigquery.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.tshack2022.bigquery.dto.PoiPoint;
import com.tshack2022.bigquery.dto.TrackPoint;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class QueryService {

    private static Long maxDistanceMeters = null;

    private BigQuery bigquery;

    public QueryService(BigQuery bigquery) {
        this.bigquery = bigquery;
    }

    public List<TrackPoint> getAllTrackPoints() {
        List<TrackPoint> trackPoints = new ArrayList<>();
        String query = "SELECT t.* FROM drivedata.track_road t;";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                TrackPoint tp = parseTrackPoint(row);
                trackPoints.add(tp);
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return trackPoints;
    }

    public Long getMaxDistanceMeters() {
        if (maxDistanceMeters != null) {
            return maxDistanceMeters;
        }
        String query = "SELECT max(t.meters) as max_distance_meters FROM drivedata.track_road t;";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                maxDistanceMeters = row.get("max_distance_meters").getLongValue();
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return maxDistanceMeters;
    }

    public Long getMaxOrderId() {
        Long max = null;
        String query = "SELECT max(t.order_id) as max_order_id FROM drivedata.track_road t;";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                max = row.get("max_order_id").getLongValue();
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return max;
    }

    public TrackPoint getNearestUnorderedPoint(Long lastOrderId) {
        TrackPoint out = null;
        String query = "SELECT a.lng, a.lat, " +
                "ARRAY_AGG(STRUCT(b.lng, b.lat) ORDER BY ST_Distance(ST_GeogPoint(a.lng, a.lat), ST_GeogPoint(b.lng, b.lat)) LIMIT 1) " +
                "[ORDINAL(1)] as neighbor_coordinates " +
                "FROM `tshack2022-3002.drivedata.track_road` a JOIN `tshack2022-3002.drivedata.track_road` b " +
                "ON ST_DWithin(ST_GeogPoint(a.lng, a.lat), ST_GeogPoint(b.lng, b.lat), " + (getMaxDistanceMeters() + 1000) +
                ") AND (a.lng != b.lng OR a.lat != b.lat) " +
                "AND a.order_id = " + lastOrderId + " AND b.order_id is null " +
                "GROUP BY a.lng, a.lat";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                String lng = row.get("lng").getStringValue();
                String lat = row.get("lat").getStringValue();
                FieldValueList neighborCoordinates = row.get("neighbor_coordinates").getRecordValue();
                String nbLng = neighborCoordinates.get("lng").getStringValue();
                String nbLat = neighborCoordinates.get("lat").getStringValue();
                out = getPointByCoordinates(nbLng, nbLat);
                System.out.println("neighborCoordinates: " + nbLng + ": " + nbLat);
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return out;
    }

    public TrackPoint getPointByCoordinates(String lng, String lat) {
        TrackPoint out = null;
        String query = "SELECT t.* FROM drivedata.track_road t WHERE t.lng = " + lng + " AND t.lat = " + lat;
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                out = parseTrackPoint(row);
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return out;
    }

    public TrackPoint getPointByOrderId(Long orderId) {
        TrackPoint out = null;
        String query = "SELECT t.* FROM drivedata.track_road t WHERE t.order_id = " + orderId;
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                out = parseTrackPoint(row);
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return out;
    }

    public void updatePointOrderId(String lng, String lat, Long orderId) {
        String query = "UPDATE `tshack2022-3002.drivedata.track_road` SET order_id = " + orderId +
                " WHERE lat = " + lat + " AND lng = " + lng;
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            bigquery.query(queryConfig);
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
    }

    private TrackPoint parseTrackPoint(FieldValueList row) {
        TrackPoint out;
        Long orderId = null;
        if (!row.get("order_id").isNull()) {
            orderId = row.get("order_id").getLongValue();
        }
        String lng = row.get("lng").getStringValue();
        String lat = row.get("lat").getStringValue();
        String meters = row.get("meters").getStringValue();
        String seconds = row.get("seconds").getStringValue();
        String temperature = row.get("temperature").getStringValue();
        String weatherConditions = row.get("weather_conditions").getStringValue();
        String visibility = row.get("visibility").getStringValue();
        String roadConditions = row.get("road_conditions").getStringValue();
        out = TrackPoint.builder().lng(lng).lat(lat).meters(meters).seconds(seconds).temperature(temperature)
                .weatherConditions(weatherConditions).visibility(visibility).roadConditions(roadConditions)
                .orderId(orderId).build();
        return out;
    }

    public List<PoiPoint> getNearestPois(String lng, String lat) {
        List<PoiPoint> out = new ArrayList<>();
        String query = "SELECT p.lng, p.lat, p.sightseeing FROM `tshack2022-3002.drivedata.pois` p " +
                "WHERE ST_DWithin(ST_GeogPoint(" + lng + ", " + lat + "), ST_GeogPoint(p.lng, p.lat), 25000)";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                PoiPoint point = parsePoiPoint(row);
                out.add(point);
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return out;
    }

    public List<PoiPoint> getAllPois() {
        List<PoiPoint> out = new ArrayList<>();
        String query = "SELECT p.* FROM `tshack2022-3002.drivedata.pois` p";
        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(query).build();
        try {
            for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
                PoiPoint point = parsePoiPoint(row);
                out.add(point);
            }
        } catch (InterruptedException ex) {
            System.err.println(ex.getMessage());
        }
        return out;
    }

    private PoiPoint parsePoiPoint(FieldValueList row) {
        String poiLng = row.get("lng").getStringValue();
        String poiLat = row.get("lat").getStringValue();
        String poiSightseeing = row.get("sightseeing").getStringValue();
        return PoiPoint.builder().lng(poiLng).lat(poiLat).sightseeing(poiSightseeing).build();
    }

    public List<TrackPoint> orderTrackPoints() {
        List<TrackPoint> trackPoints = new ArrayList<>();
        Long max = getMaxOrderId();
        TrackPoint next = getNearestUnorderedPoint(max);
        while (next != null) {
            System.out.println("Max: " + max);
            System.out.println("Neighbour: " + next);
            max++;
            updatePointOrderId(next.getLng(), next.getLat(), max);
            trackPoints.add(getPointByCoordinates(next.getLng(), next.getLat()));
            next = getNearestUnorderedPoint(max);
        }
        return trackPoints;
    }

}
