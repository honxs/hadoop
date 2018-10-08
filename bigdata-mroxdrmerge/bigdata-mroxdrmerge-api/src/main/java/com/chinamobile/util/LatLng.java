package com.chinamobile.util;

/**
 * Created by Zhou Xingwei on 2015-09-14.
 */

@NotProguard
public class LatLng
{
    public double longitude;
    public double latitude;

    public LatLng(double latitude, double longitude)
    {
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public LatLng()
    {
        this.longitude = 0.0D;
        this.latitude = 0.0D;
    }

    @Override
    public String toString()
    {
        return "("+String.valueOf(longitude)+","+String.valueOf(latitude)+")";
    }

}



