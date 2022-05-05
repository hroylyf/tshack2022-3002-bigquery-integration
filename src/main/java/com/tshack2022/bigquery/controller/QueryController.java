package com.tshack2022.bigquery.controller;

import com.tshack2022.bigquery.dto.PoiPoint;
import com.tshack2022.bigquery.dto.TrackPoint;
import com.tshack2022.bigquery.service.QueryService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@Transactional
public class QueryController {

    private QueryService queryService;

    public QueryController(QueryService queryService) {
        this.queryService = queryService;
    }

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/points",
            produces = {"application/json"}
    )
    public ResponseEntity<List<TrackPoint>> getTrackPoints() {
        List<TrackPoint> out = new ArrayList<>();
        out = queryService.getAllTrackPoints();
        return new ResponseEntity<List<TrackPoint>>(out, HttpStatus.OK);
    }

    @RequestMapping(
            method = RequestMethod.POST,
            value = "/points/order",
            produces = {"application/json"}
    )
    public ResponseEntity<List<TrackPoint>> orderTrackPoints() {
        List<TrackPoint> out = new ArrayList<>();
        out = queryService.orderTrackPoints();
        return new ResponseEntity<List<TrackPoint>>(out, HttpStatus.OK);
    }

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/points/by-order",
            produces = {"application/json"}
    )
    public ResponseEntity<TrackPoint> getPointByOrderId(@RequestParam(value = "orderId", required = true) Long orderId) {
        TrackPoint out = queryService.getPointByOrderId(orderId);
        return new ResponseEntity<>(out, HttpStatus.OK);
    }

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/pois/around",
            produces = {"application/json"}
    )
    public ResponseEntity<List<PoiPoint>> getNearestPois(@RequestParam(value = "lng", required = true) String lng, @RequestParam(value = "lat", required = true) String lat) {
        List<PoiPoint> out = queryService.getNearestPois(lng, lat);
        return new ResponseEntity<>(out, HttpStatus.OK);
    }

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/pois/all",
            produces = {"application/json"}
    )
    public ResponseEntity<List<PoiPoint>> getAllPois() {
        List<PoiPoint> out = queryService.getAllPois();
        return new ResponseEntity<>(out, HttpStatus.OK);
    }

}
