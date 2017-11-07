package com.lovecws.mumu.storm.trident;

import java.io.Serializable;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 医疗诊断
 * @date 2017-11-06 17:03
 */
public class DiagnosisEvent implements Serializable {
    private double lng;
    private double lat;
    private long time;
    private String code;

    public DiagnosisEvent(final double lng, final double lat, final long time, final String code) {
        this.lng = lng;
        this.lat = lat;
        this.time = time;
        this.code = code;
    }

    public double getLng() {
        return lng;
    }

    public void setLng(final double lng) {
        this.lng = lng;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(final double lat) {
        this.lat = lat;
    }

    public String getCode() {
        return code;
    }

    public void setCode(final String code) {
        this.code = code;
    }

    public long getTime() {
        return time;
    }

    public void setTime(final long time) {
        this.time = time;
    }
}
