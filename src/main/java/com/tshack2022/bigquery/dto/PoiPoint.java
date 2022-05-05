package com.tshack2022.bigquery.dto;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Builder
@ToString
public class PoiPoint {

    @EqualsAndHashCode.Include
    private String lng;
    @EqualsAndHashCode.Include
    private String lat;
    private String sightseeing;

}
