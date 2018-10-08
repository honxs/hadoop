package com.chinamobile.xdr;

import com.chinamobile.util.*;

import java.text.DecimalFormat;
import java.util.List;

/**
 * Copyright © Zhou Xingwei. All Rights Reserved
 * Email: zhouxingwei@139.com
 * Function:
 * Usage:
 */
@NotProguard
public class LocationInfo extends GtpInfo
{
    public long frameIndex;
    public long frameTimeStamp;
    public long frameTimeStampSyn;
    public String innerSourceIP;
    public int innerSourcePort;
    public String innerTargetIP;
    public int innerTargetPort;

    public String outerSourceIP;
    public int outerSourcePort;
    public String outerTargetIP;
    public int outerTargetPort;

    public String sourceTEID;
    public String targetTEID;

    public String requestType;    //GET? POST? 200OK?
    public String host;       //HOST+post后path?
    public String url;
    public String locationType;     //gps?wf?cl?
    public String coorType;
    public long timeStamp;
    public double longitude;
    public double latitude;
    public double radius;
    public String indoor;

    public String networkType;
    public String locCgi;
    public List<String> wifiList;
    public String imsi = "";

    @Override
    public String toString()
    {
        return "LocationInfo{" +
                "frameIndex=" + frameIndex +
                ", frameTimeStamp='" + TimeUtil.getTimeStringFromMillis(frameTimeStamp, "yyyy-MM-dd HH:mm:ss.SSS") + '\'' +
                ", innerSourceIP='" + innerSourceIP + '\'' +
                ", innerSourcePort=" + innerSourcePort +
                ", innerTargetIP='" + innerTargetIP + '\'' +
                ", innerTargetPort=" + innerTargetPort +
                ", outerSourceIP='" + outerSourceIP + '\'' +
                ", outerSourcePort=" + outerSourcePort +
                ", outerTargetIP='" + outerTargetIP + '\'' +
                ", outerTargetPort=" + outerTargetPort +
                ", sourceTEID='" + sourceTEID + '\'' +
                ", targetTEID='" + targetTEID + '\'' +
                ", _enbS1APId='" + super._enbS1ApId + '\'' +
                ", _mmeS1APId='" + super._mmeS1ApId + '\'' +
                ", _cgi='" + super._cgi + '\'' +
                ", _TMSI='" + super._tmsi + '\'' +
                ", requestType='" + requestType + '\'' +
                ", host='" + host + '\'' +
                ", url='" + url + '\'' +
                ", locationType='" + locationType + '\'' +
                ", coorType='" + coorType + '\'' +
                ", timeStamp='" + TimeUtil.getTimeStringFromMillis(timeStamp, "yyyy-MM-dd HH:mm:ss") + '\'' +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", radius=" + radius +
                ", indoor='" + indoor + '\'' +
                ", networkType='" + networkType + '\'' +
                ", locCgi='" + locCgi + '\'' +
                ", wifiList=" + wifiList +
                ", enodeB=" +NetworkUtil.getEnbId(super._cgi, 10) +
                '}';
    }

    public String getBMap()
    {
        if(GisUtil.isValid(latitude,longitude))
        {
            LatLng tmpBaiduLoc = GisUtil.GPS2Baidu(new LatLng(latitude, longitude));
            DecimalFormat df=new DecimalFormat(".######");
            return ("points.push(new BMap.Point(" + df.format(tmpBaiduLoc.longitude) + "," + df.format(tmpBaiduLoc.latitude) + "));\r\n");
        }
        else
            return "\r\n";
    }


//    @Override
//    public String toString()
//    {
//        return "LocationInfo{" +
//                "frameIndex=" + frameIndex +
//                ", frameTimeStamp=" + frameTimeStamp +
//                ", innerSourceIP='" + innerSourceIP + '\'' +
//                ", innerSourcePort=" + innerSourcePort +
//                ", innerTargetIP='" + innerTargetIP + '\'' +
//                ", innerTargetPort=" + innerTargetPort +
//                ", outerSourceIP='" + outerSourceIP + '\'' +
//                ", outerSourcePort=" + outerSourcePort +
//                ", outerTargetIP='" + outerTargetIP + '\'' +
//                ", outerTargetPort=" + outerTargetPort +
//                ", sourceTEID='" + sourceTEID + '\'' +
//                ", targetTEID='" + targetTEID + '\'' +
//                ", requestType='" + requestType + '\'' +
//                ", host='" + host + '\'' +
//                ", url='" + url + '\'' +
//                ", locationType='" + locationType + '\'' +
//                ", coorType='" + coorType + '\'' +
//                ", timeStamp=" + timeStamp +
//                ", longitude=" + longitude +
//                ", latitude=" + latitude +
//                ", radius=" + radius +
//                ", indoor='" + indoor + '\'' +
//                ", networkType='" + networkType + '\'' +
//                ", locCgi='" + locCgi + '\'' +
//                ", wifiList=" + wifiList +
//                '}';
//    }
}
