package com.tshack2022.bigquery.dto;

import lombok.*;

@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@Builder
@ToString
public class TrackPoint {

    private Long orderId;
    @EqualsAndHashCode.Include
    private String lng;
    @EqualsAndHashCode.Include
    private String lat;
    private String meters;
    private String seconds;
    private String temperature;
    private String weatherConditions;
    private String visibility;
    private String roadConditions;

    /*@Override
    public boolean equals(Object other) {
        if (!(other instanceof TrackPoint))
            return false;
        TrackPoint otherPoint = (TrackPoint) other;
        return otherPoint.getLat().equals(getLat()) && otherPoint.getLng().equals(getLng());
    }*/

    /*@Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (getLng() == null ? 0 : getLng().hashCode());
        hash = 31 * hash + (getLat() == null ? 0 : getLat().hashCode());
        return hash;
    }*/

}
