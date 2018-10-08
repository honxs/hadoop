package com.chinamobile.util;

/**
 * Created by Zhou Xingwei on 2015-09-14.
 */


public class GisUtil
{

    public static boolean isValid(LatLng location)
    {
        return location!=null && location.longitude < 135 && location.longitude > 75 && location.latitude > 16 && location.latitude < 60;
    }

    public static boolean isValid(double lat, double lon)
    {
        return lon < 135 && lon > 75 && lat > 16 && lat < 60;
    }



    //两点距离
    public static double distance(LatLng pointA, LatLng pointB)
    {
        return distance(pointA.latitude,pointA.longitude,pointB.latitude,pointB.longitude);
    }


    public static double distance(double latA, double logA, double latB,double  logB){
        int earthR = 6371000;
        double x = Math.cos(latA*Math.PI/180) * Math.cos(latB*Math.PI/180) * Math.cos((logA-logB)*Math.PI/180);
        double y = Math.sin(latA*Math.PI/180) * Math.sin(latB*Math.PI/180);
        double s = x + y;
        if (s > 1)
            s = 1;
        if (s < -1)
            s = -1;
        double alpha = Math.acos(s);
        double distance = alpha * earthR;
        return distance;
    }

    public static double angle(double lng_a, double lat_a, double lng_b,double  lat_b){
        //计算A->B线段的方位角
        double d = 0;
        if (Math.abs(lng_a - lng_b) > 0.000001 || Math.abs(lat_a - lat_b) > 0.000001)
        {
            lat_a=lat_a*Math.PI/180;
            lng_a=lng_a*Math.PI/180;
            lat_b=lat_b*Math.PI/180;
            lng_b=lng_b*Math.PI/180;
            d=Math.sin(lat_a)*Math.sin(lat_b)+Math.cos(lat_a)*Math.cos(lat_b)*Math.cos(lng_b-lng_a);
            d=Math.sqrt(1-d*d);
            d=Math.cos(lat_b)*Math.sin(lng_b-lng_a)/d;
            d=Math.asin(d)*180/Math.PI;//d = Math.round(d*10000);
//
            if(lng_a<lng_b && lat_a>lat_b)
                d = 180-d;
            else if(lng_a>=lng_b && lat_a>=lat_b)
                d = 180-d;
            else if(lng_a>lng_b && lat_a<lat_b)
                d = 360+d;
        }
        return d;
    }

    public static double smallAngle(double angle1, double angle2)
    {
        double delta = Math.abs(angle1-angle2);
        if(delta>180)
        {
            delta = 360-delta;
        }
        return delta;
    }


    public static String fangwei(double angle)
    {
        if(angle>=0&&angle<10)
        {
            return "正北方向";
        }
        else if(angle>=10&&angle<80)
        {
            return "东北方向";
        }
        else if(angle>=80&&angle<100)
        {
            return "正东方向";
        }
        else if(angle>=100&&angle<170)
        {
            return "东南方向";
        }
        else if(angle>=170&&angle<190)
        {
            return "正南方向";
        }
        else if(angle>=190&&angle<260)
        {
            return "西南方向";
        }
        else if(angle>=260&&angle<280)
        {
            return "正西方向";
        }
        else if(angle>=280&&angle<350)
        {
            return "西北方向";
        }
        else if(angle>=350&&angle<360)
        {
            return "正北方向";
        }
        else
        {
            return "";
        }
    }

    public static LatLng GPS2Baidu(LatLng gpsLocation)
    {
        if(gpsLocation==null)
        {
            return null;
        }
        double gpsLat = gpsLocation.latitude;
        double gpsLon = gpsLocation.longitude;
        double[] baidu = CoordinateConvert.wgs2BD09(gpsLat,gpsLon);
        LatLng baiduLocation = new LatLng(baidu[0],baidu[1]);
        return baiduLocation;
    }

    public static LatLng Baidu2GPS(LatLng baiduLocation)
    {
        if (baiduLocation == null)
        {
            return null;
        }
        double baiduLat = baiduLocation.latitude;
        double baiduLon = baiduLocation.longitude;
        double[] gps = CoordinateConvert.bd092WGS(baiduLat, baiduLon);
        LatLng gpsLocation = new LatLng(gps[0], gps[1]);
        return gpsLocation;
    }
}
